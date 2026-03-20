import faiss
from typing import List
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.iterables import EmbeddingsBatchIterable
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, Stages, Action, ChunkType, Embedding, Dataset

class CentroidsProducer(BaseQueueWorker):
    """
    Создает центроиды имеющихся эмбеддингов
    """
    def __init__(
            self, 
            router: DBRouter,
            ui,
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
        self.ui = ui

    async def produce(self):
        d = 768
        K = 512

        id = EmbeddingsRepository(self._router).get_max_id()
        batches = EmbeddingsBatchIterable(repo=EmbeddingsRepository(self._router), batch_size=10000)
        
        embeddings_data = []
        for batch in self.ui.tqdm(batches, total=len(batches), desc="Load embeddings"):
            for e in batch:
                embeddings_data.append(e.data)

        self.logger.info("Create KMeans")
        kmeans = faiss.Kmeans(d, K, niter=20, verbose=False)

        self.logger.info("Train KMeans")
        kmeans.train(embeddings_data)

        cluster_centers = kmeans.centroids.copy()

        self.logger.info("Clear RAM")
        del embeddings_data

        self.logger.info("Spread clusters")
        for centro in cluster_centers:
            id += 1
            task = Task(
                id=id,
                name=str(id),
                dataset=Dataset.EMBEDDING,
                routes=[Action.DB, Action.TAG],
                cost=centro.nbytes,
                entity=Embedding(
                    id=id,
                    chunk_id=id,
                    source_id=id,
                    data=centro,
                    seq=0,
                    shape=len(centro),
                    type=ChunkType.CENTROID,
                )
            )

            yield task

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[Task[Embedding]]:
        return batch