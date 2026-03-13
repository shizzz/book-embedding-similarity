import numpy as np
import faiss
from typing import List
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
            name: str = Stages.INDEX,
            *args, 
            **kwargs
        ):
        super().__init__(name=f"{name}_{level.value}", *args, **kwargs)

        self.level = level
        self.embedding_dim = None
        self.index: faiss.IndexIDMap = None
        self._count: int = 0

        self._book_repo = BookRepository(router)
        self._emb_repo = EmbeddingsRepository(router)

    def create_index(self, shape: int):
        base_index = faiss.IndexHNSWFlat(
            shape,
            IndexConfig.HNSW_M,
            faiss.METRIC_INNER_PRODUCT
        )
        base_index.hnsw.efConstruction = IndexConfig.HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = IndexConfig.HNSW_EF_SEARCH
        self.index = faiss.IndexIDMap(base_index)

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[Task]:
        ids = []
        vectors = []
        result: List[Task[int]] = []

        for b in batch:
            if self.embedding_dim is None:
                self.embedding_dim = b.entity.shape
                self.create_index(self.embedding_dim)

            # if b.entity.data.shape[0] != self.embedding_dim:
            #     raise ValueError(f"Chunk {b.entity.id} имеет dim={b.entity.data.shape[0]}, ожидается {self.embedding_dim}")
            vectors.append(b.entity.data)

            if self.level == IndexLevel.DOCUMENT:
                entity_id = b.entity.book_id
            elif self.level == IndexLevel.CHUNK:
                entity_id = b.entity.id
            else:
                raise TypeError("Unknown index type")

            ids.append(entity_id)

            result.append(
                Task(
                    id=entity_id,
                    name=str(entity_id),
                    entity=entity_id,
                    routes=Action.NONE
                )
            )

        ids_np = np.array(ids, dtype=np.int64)
        vectors_np = np.vstack(vectors)
        self.index.add_with_ids(vectors_np, ids_np)
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

        faiss.write_index(self.index, str(tmp))
        tmp.replace(path)
        
        if self.logger:
            self.logger.info(f"{self.level} индекс построен: {self._count} книг, dim={self.embedding_dim}")