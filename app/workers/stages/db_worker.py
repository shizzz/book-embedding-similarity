import asyncio
from typing import List
from enum import IntEnum
from app.common.types import TEntity
from app.infrastructure.models import Stages, BatchTask
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.models import Task

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
        self._save = save_func
        self._router = router

    async def process(self, batch: List[Task]) -> List[Task]:
        total_done = await asyncio.to_thread(self._save_batch, batch)
        return batch

    def _save_batch(self, batch: List[Task]) -> int:
        groups: dict[IntEnum | None, tuple[Task, list]] = {}

        for task in batch:
            action = task.action

            if action in groups:
                groups[action][1].append(task.entity)
            else:
                groups[action] = (task, [task.entity])

        grouped_tasks: list[BatchTask[TEntity]] = [
            base.clone(entity=entities)
            for base, entities in groups.values()
        ]

        self._save(self._router, grouped_tasks)

        return len(batch)