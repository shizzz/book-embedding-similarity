import asyncio
import numpy as np
import faiss
from typing import List
from app.hnsw import FaissId
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookRepository, EmbeddingsRepository
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Action, Embedding, Stages
from app.settings import PathsConfig, ProcessConfig, IndexConfig, IndexLevel

class Indexer(BaseQueueWorker[Embedding]):
    def __init__(
            self,
            router: DBRouter,
            level: IndexLevel,
            shape: int,
            name: str = Stages.INDEX,
            *args, 
            **kwargs
        ):
        super().__init__(name=f"{name}_{level.value}", *args, **kwargs)

        self.level = level
        self.embedding_dim = shape
        self.index: faiss.IndexIDMap = None
        self._count: int = 0

        self._book_repo = BookRepository(router)
        self._emb_repo = EmbeddingsRepository(router)

        if level == IndexLevel.DOCUMENT:
            self._get_entity_id = lambda emb: emb.book_id
        elif level == IndexLevel.CHUNK:
            self._get_entity_id = lambda emb: FaissId.pack(emb.book_id, emb.id)
        else:
            raise TypeError("Unknown index type")

    def create_index(self, shape: int):
        base_index = faiss.IndexHNSWFlat(
            shape,
            IndexConfig.HNSW_M,
            faiss.METRIC_INNER_PRODUCT
        )
        base_index.hnsw.efConstruction = IndexConfig.HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = IndexConfig.HNSW_EF_SEARCH
        self.index = faiss.IndexIDMap(base_index)

    async def before_start(self):
        if self.index is None:
            await asyncio.to_thread(self.create_index, self.embedding_dim)

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[Task]:
        ids = []
        vectors = []
        result: List[Task[int]] = []

        vectors_append = vectors.append
        ids_append = ids.append
        result_append = result.append

        # собираем данные синхронно (быстро)
        for b in batch:
            vectors_append(b.entity.data)
            entity_id = self._get_entity_id(b.entity)
            ids_append(entity_id)

            result_append(
                Task(
                    id=entity_id,
                    name=str(entity_id),
                    entity=entity_id,
                    routes=Action.NONE
                )
            )

        # переводим в numpy массивы (может быть тяжело, лучше в поток)
        ids_np, vectors_np = await asyncio.to_thread(
            lambda: (np.array(ids, dtype=np.int64), np.vstack(vectors))
        )

        # добавление в faiss индекс (тяжелая синхронная операция)
        await asyncio.to_thread(self.index.add_with_ids, vectors_np, ids_np)
        self._count += len(ids)

        return result

    async def count_total(self) -> None:
        total = None
        if self.level == IndexLevel.DOCUMENT:
            total = self._book_repo.count()
        elif self.level == IndexLevel.CHUNK:
            total = self._emb_repo.count()

        if total:
            await self.stats.set_total(self.name, total)

    async def fin(self):
        path = PathsConfig.DATA_DIR / f"{ProcessConfig.MODEL_NAME}.{self.level.value}.faiss"
        tmp = path.with_suffix(".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self.index, str(tmp))
        tmp.replace(path)
        
        if self.logger:
            self.logger.info(f"{self.level} индекс построен: {self._count} книг, dim={self.embedding_dim}")