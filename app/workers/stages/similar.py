import asyncio
from typing import List, Tuple
from app.searchEngines.similarSearch import SimilarSearchEngine
from app.services import BulkSimilarSearchService
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Action, BookTask, Similar, Stages, Dataset

class SimilarStage(BaseQueueWorker[Similar]):
    def __init__(
            self,
            engine: SimilarSearchEngine,
            name: str = Stages.SIMILAR,
            *args, 
            **kwargs
    ):
        super().__init__(
            name=name,
            *args, 
            **kwargs
        )

        self._service = BulkSimilarSearchService(engine, self.logger)
        self._id_lock = asyncio.Lock()
        self._id: int = 0

    async def process(self, batch: List[Task[BookTask]], wid: int) -> List[Task[List[Tuple[float, int, int]]]]:
        ids = [task.entity.book.id for task in batch]

        result = self._service.run(ids)
        task_id = await self._reserve_id()

        return [Task(
            id=task_id,
            name=str(task_id),
            entity=result,
            dataset=Dataset.SIMILAR,
            routes=Action.DB,
            done=len(ids)
        )]
    
    async def _reserve_id(self) -> int:
        async with self._id_lock:
            id = self._id
            self._id += 1
            return id