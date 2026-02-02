import asyncio
import gc
from app.models import Task
from app.workers import BaseWorker
from app.workers.similar_processing import SimilarProcessQueueWorker

class BackgroundProcessingWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def stat_books(self):
        return

    def process_book(self, task: Task):
        has_q = self.db.has_queue()
        if has_q:
            worker = SimilarProcessQueueWorker()
            asyncio.run(worker.run())
            del worker
            gc.collect()
