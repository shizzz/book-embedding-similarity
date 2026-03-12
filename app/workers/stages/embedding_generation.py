import asyncio
import torch
import numpy as np
from collections import defaultdict
from app.model import Model
from app.parsers.chunk import ChunkStrategyFactory
from app.workers.batchStrategies import CharFuncBatchStrategy
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, Chunk, Embedding, Action, Stages, Dataset
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
            batch_strategy=lambda: CharFuncBatchStrategy(self._batch_char_limit),
            *args, 
            **kwargs
        )

        self._model = model 
        self._max_chars = model.info.st_chunk_size
        self._overlap = model.info.st_overlap
        self._transformer_batch_size = model.info.st_batch_size
        self._min_chars = max(100, int(self._max_chars * 0.15))
        
        self._repo = EmbeddingsRepository(router, model.info.uid)
        self._emb_id = self._repo.get_max_id()       
        self._emb_id_lock = asyncio.Lock()
        self._emb_to_chunk_id = self._repo.get_ids()
        self._current_batch_size = int(self._transformer_batch_size)

    async def process(self, batch: List[Task[Chunk]], wid: int) -> List[Task[Embedding]] | None:
        chunks = [task.entity for task in batch if task.entity.chunk_id not in self._emb_to_chunk_id]   
        if len(chunks) > 0:
            texts, meta = self._collect_chunks(chunks)
            embeddings = await asyncio.to_thread(self._embedding_process, texts)
 
            return await self._assign_embeddings(embeddings, meta)
        return None
    
    @staticmethod
    def batch_char_limit(chars: int, size: int) -> int:
        return chars * size

    def _batch_char_limit(self):
        return EmbeddingWorker.batch_char_limit(int(self._model.info.st_chunk_size - self._model.info.st_overlap), self._current_batch_size)

    def _embedding_process(self, batch: List[str]) -> np.ndarray:
        while True:
            try:
                embeddings = self._model.transformer.encode(
                    batch,
                    batch_size=self._current_batch_size,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False
                )
                return embeddings.astype(np.float32, copy=False)
            except torch.cuda.OutOfMemoryError:
                if self._current_batch_size == 1:
                    # Нечего делить — падаем окончательно
                    self.logger.error(f"Batch of size 1 still OOM at index")
                    raise
                
                self.logger.warning(f"OOM at batch {self._current_batch_size}, уменьшаем размер батча")
                self._current_batch_size -= 1
                self._model.info.st_batch_size = self._current_batch_size
                torch.cuda.empty_cache()

    def _collect_chunks(self, chunks: List[Chunk]) -> tuple[List[str], List[tuple[Chunk, int]]]:
        texts = []
        meta = []

        for chunk in chunks:
            if not chunk.text:
                continue
            
            strategy = ChunkStrategyFactory().create(chunk.type)
            parts = strategy.prepare(chunk.text).split(
                max_chars=self._max_chars,
                min_chars=self._min_chars,
                overlap=self._overlap,
                single_chunk_mode=False
            )

            for idx, part in enumerate(parts):
                texts.append(part)
                meta.append((chunk, idx))

        return texts, meta

    async def _assign_embeddings(self, embeddings: np.ndarray, meta: List[tuple[Chunk, int]]) -> List[Task[Embedding]]:
        tasks: List[Task] = []

        chunk_seq_counter = defaultdict(int)
        for emb_vector, (chunk, _) in zip(embeddings, meta):
            seq = chunk_seq_counter[(chunk.book_id, chunk.chunk_id)]
            chunk_seq_counter[(chunk.book_id, chunk.chunk_id)] += 1

            emb = Embedding(
                id=await self._reserve_id(),
                book_id=chunk.book_id,
                chunk_id=chunk.chunk_id,
                data=emb_vector,
                shape=emb_vector.shape[0],
                seq=seq,
                type=chunk.type,
            )

            task = Task(
                emb.id,
                ",".join(map(str, (emb.book_id, emb.chunk_id, emb.seq))),
                entity=emb,
                action=Action.EMBEDDING,
                dataset=Dataset.EMBEDDING,
            )

            tasks.append(task)
        return tasks
    
    async def _reserve_id(self) -> int:
        async with self._emb_id_lock:
            id = self._emb_id
            self._emb_id += 1
            return id