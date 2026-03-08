import asyncio
from app.infrastructure.models import Stages
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.models import Task
from typing import List

class DbWorker(BaseQueueWorker):
    """
    Stage для сохранения в базу
    """
    def __init__(
            self,
            router: DBRouter,
            save_func,
            name: str = Stages.DB,
            *args, 
            **kwargs
        ):
        super().__init__(name=name ,*args, **kwargs)
        self._save_func = save_func
        self._router = router

    async def process(self, batch: List[Task]) -> List[Task]:
        total_done = await asyncio.to_thread(self._save_batch, batch)
        return batch

    def _save_batch(self, batch: List[Task]) -> int:
        self._save_func(self._router, batch)
        return len(batch)
