import logging
from app.infrastructure.db import Migrator
from app.infrastructure.db.repositories import ModelRepository
from app.infrastructure.models import IndexLevel
from app.workers.pipelines import IndexPipeline
from app.workers.base import BaseWorker
from app.settings import ProcessConfig

class GenerateIndexWorker(BaseWorker):
    def __init__(self, level: IndexLevel, *args, **kwargs):
        super().__init__(name="Generate index", logger=logging.getLogger(__name__), *args, **kwargs)

        model_settings = ProcessConfig.MODEL_NAME
        uid, model_db = ModelRepository(self.router).get_latest_uid(ProcessConfig.MODEL_NAME)
        Migrator(self.router).migrate_embeddings(uid)

        self._level = level

        if model_settings != model_db:
            self.logger.warning(f"Diffrent model in settings [{model_settings}] and DB [{model_db}]")
        self.logger.info(f"Selected model: {model_settings}")

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        indexPipeline = IndexPipeline(
            self._level,
            router=self.router,
            stats=self.stats,
            logger=self.logger,
        )
        self.pipelines.append(indexPipeline)