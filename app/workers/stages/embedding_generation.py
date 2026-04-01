import asyncio
import torch
import numpy as np
from app.model import Model
from app.workers.batchStrategies import TokenChunkBatchStrategy
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, TokenChunk, Embedding, Action, Stages, Dataset
from typing import List

class EmbeddingWorker(BaseQueueWorker):
    """
    Stage для сохранения в базу
    """
    def __init__(
            self,
            model: Model,
            router: DBRouter,
            name: str = Stages.EMBEDDING,
            *args, 
            **kwargs
        ):
        super().__init__(
            name=name,
            batch_strategy=lambda: TokenChunkBatchStrategy(self._batch_char_limit),
            *args, 
            **kwargs
        )

        self._model = model 
        self._device = next(model.model.parameters()).device
        self._transformer_batch_size = model.info.st_batch_size
        
        self._repo = EmbeddingsRepository(router, model.info.uid)
        self._emb_id = self._repo.get_max_id()       
        self._emb_id_lock = asyncio.Lock()

        self._vram_increasing = True
        self._vram_increase_iter: int = 0
        self._tokens_lock = asyncio.Lock()

    @staticmethod
    def batch_char_limit(chars: int, batch: int) -> int:
        return int(chars * batch)

    def _batch_char_limit(self):
        return EmbeddingWorker.batch_char_limit(
            self._model.info.tokens_per_batch, 
            self.batch_size
        )

    async def process(self, batch: List[Task[TokenChunk]], wid: int) -> List[Task[Embedding]] | None:
        chunks = [t.entity for t in batch]
        if not chunks:
            return None

        embeddings = await self._adaptive_embedding_process(chunks)
        return await self._assign_embeddings(embeddings, chunks)
    
    async def _adaptive_embedding_process(self, batch: List[TokenChunk]) -> np.ndarray:
        divider = 1

        while True:
            try:
                # делим батч
                if divider > 1:
                    split_batches = np.array_split(batch, divider)
                    results = []
                    for sub in split_batches:
                        if len(sub) == 0:
                            continue
                        result = await asyncio.to_thread(self._embedding_process, list(sub))
                        results.append(result)
                    embeddings = np.vstack(results)
                else:
                    embeddings = await asyncio.to_thread(self._embedding_process, batch)

                # успех — проверяем VRAM
                if self._vram_increasing:
                    await self._increase_tokens(sum([chunk.length for chunk in batch]))

                return embeddings

            except RuntimeError as e:
                if "out of memory" not in str(e).lower():
                    raise
                torch.cuda.empty_cache()

                divider += 1

                # уменьшаем tokens_per_batch
                await self._decrease_tokens()

                # защита от бесконечного деления
                if divider > len(batch):
                    raise RuntimeError("Batch cannot be split further — still OOM")   

    async def _decrease_tokens(self):
        async with self._tokens_lock:
            if self._model.info.tokens_per_batch <= 1:
                return

            new_val = int(self._model.info.tokens_per_batch * 0.98)
            new_val = max(1, new_val)

            self._vram_increasing = False
            self._model.info.tokens_per_batch = new_val
            self._model.info.decreases +=1

    async def _increase_tokens(self, current_length: int):
        async with self._tokens_lock:
            self._vram_increase_iter += 1
            if self._vram_increase_iter < 10:
                return
            
            free = self._model.info.free_vram_mb
            total = self._model.info.total_vram_mb

            if not free or not total:
                return

            free_ratio = free / total

            # только если свободной памяти достаточно
            if free_ratio > self._model.info.free_vram_ratio:
                # расчет безопасного увеличения с учетом текущего батча
                proposed = int(self._model.info.tokens_per_batch * 1.01)
                max_safe = max(current_length, proposed)  # не меньше текущего
                self._model.info.tokens_per_batch = max_safe
                self._vram_increase_iter = 0
                self._model.info.increases +=1

    @staticmethod
    def _mean_pooling(last_hidden_state, attention_mask):
        mask = attention_mask.unsqueeze(-1).type_as(last_hidden_state)
        summed = (last_hidden_state * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1e-9)
        return summed / counts

    def _embedding_process(self, batch: List[TokenChunk]) -> np.ndarray:
        model = self._model.model
        tokenizer = self._model.tokenizer

        max_len = max(len(tc.tokens) for tc in batch)

        input_ids = []
        attention_mask = []

        for tc in batch:
            ids = tc.tokens
            pad_len = max_len - len(ids)

            input_ids.append(ids + [tokenizer.pad_token_id] * pad_len)
            attention_mask.append([1] * len(ids) + [0] * pad_len)

        input_ids = torch.as_tensor(input_ids, device=self._device)
        attention_mask = torch.as_tensor(attention_mask, device=self._device)

        with torch.inference_mode():
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            pooled = self._mean_pooling(outputs.last_hidden_state, attention_mask)
            pooled = torch.nn.functional.normalize(pooled, p=2, dim=1)

        return pooled.cpu().numpy()

    async def _assign_embeddings(
        self, embeddings: np.ndarray, chunks: List[TokenChunk]
    ) -> List[Task[Embedding]]:
        tasks: List[Task[Embedding]] = []

        for emb_vector, chunk in zip(embeddings, chunks):
            emb_id = await self._reserve_id()

            emb = Embedding(
                id=emb_id,
                source_id=chunk.book_id,
                chunk_id=chunk.chunk_id,
                seq=chunk.seq,
                type=chunk.type,
                data=emb_vector,
                shape=emb_vector.shape[0],
            )

            task = Task(
                id=emb.id,
                name=f"{emb.source_id},{emb.chunk_id},{emb.seq}",
                entity=emb,
                routes=Action.DB,
                dataset=Dataset.EMBEDDING,
                cost=emb.data.nbytes
            )

            tasks.append(task)

        return tasks
    
    async def _reserve_id(self) -> int:
        async with self._emb_id_lock:
            id = self._emb_id
            self._emb_id += 1
            return id