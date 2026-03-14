from typing import List
from enum import IntEnum
from app.common.types import TEntity
from app.infrastructure.models import Stages, BatchTask
from app.workers.base import BaseQueueWorker, SaveRegistry
from app.infrastructure.db import DBRouter
from app.infrastructure.models import Task

class DbWorker(BaseQueueWorker):
    """
    Stage для сохранения в базу
    """
    def __init__(
            self,
            router: DBRouter,
            registry: SaveRegistry,
            name: str = Stages.DB,
            *args, 
            **kwargs
        ):
        super().__init__(name=name ,*args, **kwargs)
        self._registry = registry
        self._router = router

    async def process(self, batch: List[Task], wid: int) -> List[Task]:
        await self._save_batch(batch)
        return batch
    
    async def _save(self, router: DBRouter, tasks: list[BatchTask[TEntity]]):
        with router.transaction() as tx:
            for task in tasks:
                saver = self._registry.get(task.dataset)

                if saver is None:
                    raise RuntimeError(f"No saver for {task.dataset}")

                await saver(task.entity, tx)

    async def _save_batch(self, batch: List[Task]) -> int:
        groups: dict[IntEnum | None, tuple[Task, list]] = {}

        for task in batch:
            dataset = task.dataset

            if dataset in groups:
                groups[dataset][1].append(task.entity)
            else:
                groups[dataset] = (task, [task.entity])

        grouped_tasks: list[BatchTask[TEntity]] = [
            base.clone(entity=entities)
            for base, entities in groups.values()
        ]

        await self._save(self._router, grouped_tasks)

        return len(batch)