import logging
from app.infrastructure.db import Migrator
from app.infrastructure.db.repositories import ModelRepository
from app.workers.pipelines import IndexPipeline
from app.workers.base import BaseWorker
from app.settings import IndexConfig, ProcessConfig

class GenerateIndexWorker(BaseWorker):
    def __init__(self):
        super().__init__(name="Generate index", logger=logging.getLogger(__name__))
        uid = ModelRepository(self.router).get_latest_uid(ProcessConfig.MODEL_NAME)
        Migrator(self.router).migrate_embeddings(uid)

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        embPipeline = IndexPipeline(
            IndexConfig.BUILD_INDEX_LEVEL,
            router=self.router,
            stats=self.stats,
            logger=self.logger,
        )
        self.pipelines.append(embPipeline)