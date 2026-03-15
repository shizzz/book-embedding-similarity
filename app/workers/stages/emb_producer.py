from typing import List
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, Embedding, Stages, Action, ChunkType

REPO_BATCH_SIZE: int = 10000

class EmbeddingProducer(BaseQueueWorker[Embedding]):
    """
    Читает книги из источника библиотеки
    """
    def __init__(
            self,
            router: DBRouter,
            name: str = Stages.EMBEDDING,
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)
        self._repo = EmbeddingsRepository(router)

    async def produce(self):
        for batch in self._repo.get_all(ChunkType.TEXT, REPO_BATCH_SIZE):
            for e in batch:
                yield Task[Embedding](
                    id=e.id,
                    name=e.id,
                    entity=e,
                    routes=[Action.GRAB, Action.INDEX],
                )

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[Task[Embedding]]:
        return batch

    async def count_total(self) -> None:
        total = self._repo.count()
        await self.stats.set_total(self.name, total)
    
    def reserve_id(self) -> int:
        id = self._book_id
        self._book_id += 1
        return id