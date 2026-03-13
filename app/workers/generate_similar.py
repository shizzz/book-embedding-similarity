import logging
from app.workers.pipelines import SimilarSearchPipeline
from app.workers.base import BaseWorker

class GenerateSimilarWorker(BaseWorker):
    def __init__(self, batch: int):
        super().__init__(name="Generate similar", logger=logging.getLogger(__name__))

    async def after_run(self) -> None:
        pass

    async def setup_stages(self):
        pipeline = SimilarSearchPipeline(
            router=self.router,
            stats=self.stats,
            logger=self.logger,
        )
        self.pipelines.append(pipeline)
