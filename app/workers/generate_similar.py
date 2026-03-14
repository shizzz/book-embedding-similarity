import asyncio
import logging
from app.workers.pipelines import SimilarSearchPipeline, DbPipeline
from app.workers.base import BaseWorker
from app.infrastructure.models import Channel, Stages

class GenerateSimilarWorker(BaseWorker):
    def __init__(self, batch: int):
        super().__init__(name="Generate similar", logger=logging.getLogger(__name__))

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        pipeline = SimilarSearchPipeline(
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            output_channels = [channel_db],
        )

        dbPipeline = DbPipeline(
            threads=1,
            batch_size=1,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            input_channel=channel_db,
        )
        self.pipelines.append(pipeline)
        self.pipelines.append(dbPipeline)
