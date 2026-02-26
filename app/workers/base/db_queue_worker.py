from abc import ABC, abstractmethod
from app.common.types import TEntity
from app.models import Task, TaskResult
from .queue_worker import BaseQueueWorker
from app.workers.sources import DbQueue

class BaseDbQueueWorker(BaseQueueWorker[TEntity], ABC):
    def __init__(
        self,
        db_queue_batch_size: int,
        db_queue_max_size: int,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self._db_queue = DbQueue(
            self.save_to_db,
            db_queue_batch_size,
            db_queue_max_size,
            self.ui
        )
    
    @abstractmethod
    def save_to_db(self, task: Task) -> int:
        pass

    async def before_run(self):    
        self._db_queue.start()

    async def before_fin(self) -> None:
        await self._db_queue.stop()
    
    async def post_process(self, result: TaskResult) -> None:
        await self._db_queue.put(result.db_queue_count, result.to_task())