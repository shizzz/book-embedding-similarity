import asyncio
import logging
from app.parsers.book import ParserConfig
from app.infrastructure.db import Migrator
from app.infrastructure.models import Channel, Stages
from app.workers.pipelines import BookScanPipeline, DbPipeline
from app.workers.base import BaseWorker

class ParseBooks(BaseWorker):
    def __init__(self, config: ParserConfig):
        super().__init__(name="Parse books", logger=logging.getLogger(__name__))
        
        Migrator(self.router).migrate_meta()
        Migrator(self.router).migrate_chunks()

        self._config = config

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        bookPipeline = BookScanPipeline(
            cnf=self._config,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            output_channels = [channel_db],
        )

        dbPipeline = DbPipeline(
            threads=1,
            batch_size=256,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            input_channel=channel_db,
        )

        self.pipelines.append(bookPipeline)
        self.pipelines.append(dbPipeline)