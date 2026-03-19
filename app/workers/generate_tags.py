import gc
import asyncio
import logging
import faiss
from typing import List
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.db.iterables import EmbeddingsBatchIterable
from app.infrastructure.models import ChunkType
from app.workers.pipelines import TaggerPipeline, DbPipeline
from app.workers.base import BaseWorker
from app.infrastructure.models import Channel, Stages, Task, Embedding, Dataset, Action

class GenerateTags(BaseWorker):
    def __init__(self):
        super().__init__(name="Generate tags", logger=logging.getLogger(__name__))

        self._channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))
        self._channel_tag = Channel(Stages.TAG, asyncio.Queue(100))

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):

        tagger_pipeline = TaggerPipeline(
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            input_channel=self._channel_tag,
            output_channels = [self._channel_db],
        )

        dbPipeline = DbPipeline(
            model_name=tagger_pipeline.model.info.model_name,
            model_uid=tagger_pipeline.model.info.uid,
            threads=1,
            batch_size=256,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            input_channel=self._channel_db,
        )
        self.pipelines.append(tagger_pipeline)
        self.pipelines.append(dbPipeline)

    async def _enqueue_task(self, tasks: List[Task]):
        for task in tasks:
            await self._channel_db.queue.put(task)
            await self._channel_tag.queue.put(task)

    async def before_run(self) -> None:
        d = 768
        K = 512

        id = EmbeddingsRepository(self.router).get_max_id()
        batches = EmbeddingsBatchIterable(repo=EmbeddingsRepository(self.router), batch_size=10000)
        
        embeddings_data = []
        for batch in self.ui.tqdm(batches, total=len(batches), desc="Processing embeddings"):
            for e in batch:
                embeddings_data.append(e.data)

        kmeans = faiss.Kmeans(d, K, niter=20, verbose=True)
        kmeans.train(embeddings_data)
        cluster_centers = kmeans.centroids.copy()

        del embeddings_data
        gc.collect()

        tasks = []
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
                    chunk_id=None,
                    data=centro,
                    seq=0,
                    shape=len(centro),
                    type=ChunkType.CENTROID,
                    source_id=None,

                )
            )
        asyncio.create_task(self._enqueue_task(tasks))
