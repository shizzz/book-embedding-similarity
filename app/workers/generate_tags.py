import asyncio
import logging
from typing import List
from app.infrastructure.models import Channel, Stages, Task
from app.workers.pipelines import TaggerPipeline, DbPipeline
from app.workers.base import BaseWorker
from app.workers.stages import CentroidsProducer

class GenerateTags(BaseWorker):
    def __init__(self):
        super().__init__(name="Generate tags", logger=logging.getLogger(__name__))

        self._channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))
        self._channel_tag = Channel(Stages.TAG, asyncio.Queue(10))

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

        self._model_name = tagger_pipeline.model.info.model_name
        self._uid = tagger_pipeline.model.info.uid

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
        centroid_producer_stage = CentroidsProducer(
            router=self.router,
            ui=self.ui,
            output_channels=[self._channel_tag, self._channel_db],
            stats=self.stats,
            logger=self.logger, 
        )

        await centroid_producer_stage.start()
        asyncio.create_task(centroid_producer_stage.wait())
        await centroid_producer_stage.input_channel.upstream_done.wait()
