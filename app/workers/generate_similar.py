import asyncio
import logging
from app.workers.pipelines import SimilarSearchPipeline, DbPipeline
from app.workers.base import BaseWorker
from app.infrastructure.models import Channel, Stages, SearchIndexLevel

class GenerateSimilarWorker(BaseWorker):
    def __init__(
        self,
        level: SearchIndexLevel,
        top_k: int,
        exclude_same_authors: bool
    ):
        super().__init__(name="Generate similar", logger=logging.getLogger(__name__))
        self._level = level
        self._top_k = top_k
        self._exclude_same_authors = exclude_same_authors

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        pipeline = SimilarSearchPipeline(
            level=self._level,
            top_k=self._top_k,
            exclude_same_authors=self._exclude_same_authors,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            output_channels = [channel_db],
        )

        dbPipeline = DbPipeline(
            threads=1,
            batch_size=10,
            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,
            input_channel=channel_db,
        )
        self.pipelines.append(pipeline)
        self.pipelines.append(dbPipeline)
