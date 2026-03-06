import asyncio
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.models import Task, TaskResult
from typing import List

class DbWorker(BaseQueueWorker):
    """
    Stage для сохранения в базу
    """
    def __init__(
            self,
            router: DBRouter,
            save_func,
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)
        self._save_func = save_func
        self._router = router

    async def process(self, batch: List[Task]) -> List[TaskResult]:
        # сохраняем batch в БД в отдельном thread
        total_done = await asyncio.to_thread(self._save_batch, batch)
        # возвращаем TaskResult для post_process / fan-out
        return [TaskResult(task=t.to_result(total_done), done=1) for t in batch]

    def _save_batch(self, batch: List[Task]) -> int:
        entities = [e.entity for e in batch]
        self._save_func(self._router, entities)
        return len(entities)
