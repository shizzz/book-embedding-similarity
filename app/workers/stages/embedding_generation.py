import asyncio
import torch
import numpy as np
from collections import defaultdict
from app.model import Model
from app.parsers.chunk import ChunkStrategyFactory
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, Chunk, Embedding, Action, Stages
from typing import List

MEM_SAFETY_MARIGN: float = 0.55

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
        super().__init__(name=name, *args, **kwargs)

        self._model = model 
        self._max_chars = model.info.st_chunk_size
        self._overlap = model.info.st_overlap
        self._transformer_batch_size = model.info.st_batch_size
        self._min_chars = max(100, int(self._max_chars * 0.15))
        
        self._repo = EmbeddingsRepository(router, model.info.uid)
        self._emb_id = self._repo.get_max_id()       
        self._emb_id_lock = asyncio.Lock()
        self._emb_to_chunk_id = self._repo.get_ids()
        self._workers_buffers = [
            {"texts": [], "meta": [], "mem": 0.0} for _ in range(self._workers_count)
        ]

    async def process(self, batch: List[Task[Chunk]], wid: int = 0) -> List[Task[Embedding]]:
        """Процессинг батча с локальным буфером воркера"""
        tasks_out: List[Task[Embedding]] = []
        buffer = self._workers_buffers[wid]
        max_mem_mb = self._max_safe_mem_mb()
        per_chunk_mb = self._model.info.estimate_mem_per_chunk_mb

        chunks = [task.entity for task in batch if task.entity not in self._emb_to_chunk_id]
        if not chunks:
            return []

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

            estimated_chunk_mem = len(parts) * per_chunk_mb

            # Если новый chunk не помещается в буфер — отправляем
            if buffer["mem"] + estimated_chunk_mem > max_mem_mb and buffer["texts"]:
                embeddings = await asyncio.to_thread(self._embedding_process, buffer["texts"])
                tasks_out.extend(await self._assign_embeddings(embeddings, buffer["meta"]))
                buffer["texts"] = []
                buffer["meta"] = []
                buffer["mem"] = 0.0

            # Добавляем chunk в буфер
            for idx, part in enumerate(parts):
                buffer["texts"].append(part)
                buffer["meta"].append((chunk, idx))
            buffer["mem"] += estimated_chunk_mem

        # Отправляем, если накоплено >90% лимита
        if buffer["mem"] >= max_mem_mb * 0.9:
            embeddings = await asyncio.to_thread(self._embedding_process, buffer["texts"])
            tasks_out.extend(await self._assign_embeddings(embeddings, buffer["meta"]))
            buffer["texts"] = []
            buffer["meta"] = []
            buffer["mem"] = 0.0

        return tasks_out

    def _embedding_process(self, batch: List[str]) -> np.ndarray:
        """Делаем encode батчами для безопасности, чтобы не вылетало OOM."""
        batch_size = self._transformer_batch_size
        embeddings = []

        for i in range(0, len(batch), batch_size):
            sub_batch = batch[i:i + batch_size]
            emb = self._model.transformer.encode(
                sub_batch,
                batch_size=len(sub_batch),
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False
            )
            embeddings.append(emb)
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        return np.vstack(embeddings).astype(np.float32, copy=False)

    def _max_safe_mem_mb(self) -> float:
        """Возвращает безопасный объём памяти в МБ для батча, учитывая занятость PyTorch."""
        if not torch.cuda.is_available():
            return 1024.0  # fallback для CPU

        props = torch.cuda.get_device_properties(0)
        total_mem = props.total_memory
        used_mem = torch.cuda.memory_allocated() + torch.cuda.memory_reserved()
        free_mem_mb = (total_mem - used_mem) / (1024 ** 2)
        return free_mem_mb * MEM_SAFETY_MARIGN  # safety margin 55%

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
            )

            tasks.append(task)
        return tasks
    
    async def _reserve_id(self) -> int:
        async with self._emb_id_lock:
            id = self._emb_id
            self._emb_id += 1
            return id