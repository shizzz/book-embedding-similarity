import asyncio
import faiss
from typing import List
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.iterables import EmbeddingsBatchIterable
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, Stages, Action, ChunkType, Embedding, Dataset

BATCH: int = 10000

class CentroidsProducer(BaseQueueWorker):
    """
    Создает центроиды имеющихся эмбеддингов
    """
    def __init__(
            self,
            router: DBRouter,
            centros: int = 256,
            name: str = Stages.PRODUCER + "_Centroids",
            *args, 
            **kwargs
        ):
        super().__init__(
            name=name,
            producer_qsize=0,
            *args, 
            **kwargs
        )
        
        self._router = router
        self._id = EmbeddingsRepository(self._router).get_max_id()
        self._shape = EmbeddingsRepository(self._router).get_shape()
        self._centros = centros

    async def produce(self):
        d = self._shape
        K = self._centros

        batches = EmbeddingsBatchIterable(repo=EmbeddingsRepository(self._router), batch_size=BATCH)
        
        embeddings_data = []
        async for batch in self.stats.atqdm(batches, total=len(batches) * BATCH, desc="Load embeddings"):
            for e in batch:
                embeddings_data.append(e.data)

        self.logger.info("Create KMeans")

        def _train():
            kmeans = faiss.Kmeans(d, K, niter=20, verbose=False)
            kmeans.train(embeddings_data)

            cluster_centers = kmeans.centroids.copy()
            faiss.normalize_L2(cluster_centers)

            return cluster_centers

        self.logger.info("Train KMeans")
        cluster_centers = await asyncio.to_thread(_train)

        self.logger.info("Clear RAM")
        #del embeddings_data

        self.logger.info("Spread clusters")
        for centro in cluster_centers:
            self._id += 1
            task = Task(
                id=self._id,
                name=str(self._id),
                dataset=Dataset.EMBEDDING,
                routes=[Action.INDEX, Action.DB],
                cost=centro.nbytes,
                entity=Embedding(
                    id=self._id,
                    chunk_id=self._id,
                    source_id=self._id,
                    data=centro,
                    seq=0,
                    shape=len(centro),
                    type=ChunkType.CENTROID,
                )
            )

            yield task

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[Task[Embedding]]:
        return batch