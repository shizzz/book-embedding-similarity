import logging
from app.infrastructure.db import Migrator
from app.infrastructure.db.repositories import ModelRepository
from app.workers.pipelines import IndexPipeline
from app.workers.base import BaseWorker
from app.settings import IndexConfig, ProcessConfig

class GenerateIndexWorker(BaseWorker):
    def __init__(self, config: IndexConfig):
        super().__init__(name="Generate index", logger=logging.getLogger(__name__))
        uid = ModelRepository(self.router).get_latest_uid(ProcessConfig.MODEL_NAME)
        Migrator(self.router).migrate_embeddings(uid)

        self._config = config

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        indexPipeline = IndexPipeline(
            self._config.SEARCH_INDEX_LEVEL,
            router=self.router,
            stats=self.stats,
            logger=self.logger,
        )
        self.pipelines.append(indexPipeline)