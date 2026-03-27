import asyncio
import logging
from app.workers.pipelines import EmbeddingPipeline, DbPipeline
from app.workers.base import BaseWorker
from app.workers.sources.databaseReporter import DatabaseReporter
from app.infrastructure.models import Channel, Stages

class GenerateEmbeddings(BaseWorker):
    def __init__(self, batch: int, *args, **kwargs):
        super().__init__(name="Generate embeddings", logger=logging.getLogger(__name__), *args, **kwargs)

    async def after_run(self) -> None:
        report = DatabaseReporter(self.router, self.model_uid).generate()
        self.ui.report(report)

    async def setup_stages(self):
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        embPipeline = EmbeddingPipeline(
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            output_channels = [channel_db],
        )

        self.model_uid = embPipeline.model.info.uid
        self.ui.model_info = embPipeline.model.info

        dbPipeline = DbPipeline(
            model_name=embPipeline.model.info.model_name,
            model_uid=embPipeline.model.info.uid,
            threads=1,
            batch_size=256,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            input_channel=channel_db,
        )
        self.pipelines.append(embPipeline)
        self.pipelines.append(dbPipeline)