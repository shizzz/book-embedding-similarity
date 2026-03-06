import asyncio
import torch
import numpy as np
from collections import defaultdict
from app.model import Model
from app.parsers.chunk import ChunkStrategyFactory
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, TaskResult, Chunk, Embedding
from typing import List

class EmbeddingWorker(BaseQueueWorker):
    """
    Stage для сохранения в базу
    """
    def __init__(
            self,
            model: Model,
            router: DBRouter,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self._model = model 
        self._max_chars = model.info.st_chunk_size
        self._overlap = model.info.st_overlap
        self._transformer_batch_size = model.info.st_batch_size
        self._min_chars = max(100, int(self._max_chars * 0.15))
        
        self._repo = EmbeddingsRepository(router, model.info.uid)
        self._emb_id = self._repo.get_max_id()
        self._emb_to_chunk_id = self._repo.get_ids()

    async def process(self, batch: List[Task[Chunk]]) -> List[TaskResult[Embedding]]:
        chunks = [task.entity for task in batch if task.entity not in self._emb_to_chunk_id]
        if len(chunks) > 0:
            return await asyncio.to_thread(self._embedding_process, chunks)
        else:
            return []
    
    def _embedding_process(self, batch: List[Chunk]) -> List[TaskResult[Embedding]]:  
        texts, meta = self._collect_chunks(batch)

        embeddings = self._model.transformer.encode(
            texts,
            batch_size=self._transformer_batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False
        )

        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        embeddings = embeddings.astype(np.float32, copy=False)

        return self._assign_embeddings(embeddings, meta)

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

    def _assign_embeddings(self, embeddings: np.ndarray, meta: List[tuple[Chunk, int]]) -> List[TaskResult[Embedding]]:
        tasks: List[TaskResult] = []

        chunk_seq_counter = defaultdict(int)
        for emb_vector, (chunk, _) in zip(embeddings, meta):
            seq = chunk_seq_counter[(chunk.book_id, chunk.chunk_id)]
            chunk_seq_counter[(chunk.book_id, chunk.chunk_id)] += 1

            emb = Embedding(
                id=self._reserve_id(),
                book_id=chunk.book_id,
                chunk_id=chunk.chunk_id,
                data=emb_vector,
                shape=emb_vector.shape[0],
                seq=seq,
                type=chunk.type,
            )

            task = TaskResult(
                emb.id,
                ",".join(map(str, (emb.book_id, emb.chunk_id, emb.seq))),
                entity=emb
            )

            tasks.append(tasks)
        return tasks
    
    def _reserve_id(self) -> int:
        id = self._emb_id
        self._emb_id += 1
        return id