from typing import List
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.db.iterables import EmbeddingsBatchIterable
from app.infrastructure.models import Task, Embedding, Stages, Action

REPO_BATCH_SIZE: int = 1000

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
        self._book_repo = EmbeddingsRepository(router)
        self._source = EmbeddingsBatchIterable(self._book_repo, REPO_BATCH_SIZE, ["book_id", "chunk_id"])

    async def produce(self):
        for batch in self._source:
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
        total = self._source.count()
        await self.stats.set_total(self.name, total)
    
    def reserve_id(self) -> int:
        id = self._book_id
        self._book_id += 1
        return id