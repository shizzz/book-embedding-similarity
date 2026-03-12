import logging
from app.infrastructure.db import router
from app.workers import stats
from app.workers.pipelines import EmbeddingPipeline
from app.workers.base import BaseWorker
from app.workers.sources.databaseReporter import DatabaseReporter

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, batch: int):
        super().__init__(name="Generate embeddings", logger=logging.getLogger(__name__))

    async def after_run(self) -> None:
        report = DatabaseReporter(self.router, self.model_uid).generate()
        self.ui.report(report)

    async def setup_stages(self):
        embPipeline = EmbeddingPipeline(
            router=self.router,
            stats=self.stats,
            logger=self.logger,
        )
        self.model_uid = embPipeline.model.info.uid
        self.pipelines.append(embPipeline)