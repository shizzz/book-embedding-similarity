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
        self._current_batch_size = int(self._transformer_batch_size)

    async def process(self, batch: List[Task[TokenChunk]], wid: int) -> List[Task[Embedding]] | None:
        chunks = [t.entity for t in batch]
        if not chunks:
            return None

        # embeddings делаем на GPU напрямую
        embeddings = await asyncio.to_thread(self._embedding_process, chunks)
        return await self._assign_embeddings(embeddings, chunks)
    
    @staticmethod
    def batch_char_limit(chars: int, batch: int) -> int:
        return int(chars * batch)

    def _batch_char_limit(self):
        return EmbeddingWorker.batch_char_limit(self._model.info.tokens_per_batch, self.batch_size)

    @staticmethod
    def _mean_pooling(last_hidden_state, attention_mask):
        mask = attention_mask.unsqueeze(-1).type_as(last_hidden_state)
        summed = (last_hidden_state * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1e-9)
        return summed / counts

    def _embedding_process(self, batch: List[TokenChunk]) -> np.ndarray:
        model = self._model.model
        tokenizer = self._model.tokenizer
        dtype = self._model.dtype

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
                book_id=chunk.book_id,
                chunk_id=chunk.chunk_id,
                seq=chunk.seq,
                type=chunk.type,
                data=emb_vector,
                shape=emb_vector.shape[0],
            )

            task = Task(
                id=emb.id,
                name=f"{emb.book_id},{emb.chunk_id},{emb.seq}",
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