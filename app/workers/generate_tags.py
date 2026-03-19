import asyncio
import logging
from app.workers.pipelines import TaggerPipeline, DbPipeline
from app.workers.base import BaseWorker
from app.infrastructure.models import Channel, Stages

class GenerateTags(BaseWorker):
    def __init__(self):
        super().__init__(name="Generate tags", logger=logging.getLogger(__name__))

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        tagger_pipeline = TaggerPipeline(
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            output_channels = [channel_db],
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
            input_channel=channel_db,
        )
        self.pipelines.append(tagger_pipeline)
        self.pipelines.append(dbPipeline)