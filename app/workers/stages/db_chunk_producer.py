from typing import List
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import ChunkRepository
from app.infrastructure.models import Task, Chunk, Stages, Action

REPO_BATCH_SIZE: int = 100

class DbChunkProducer(BaseQueueWorker[Chunk]):
    def __init__(
            self,
            router: DBRouter,
            name: str = Stages.PRODUCER + "_chunk",
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)
        self._repo = ChunkRepository(router)

    async def produce(self):
        for batch in self._repo.get_all(REPO_BATCH_SIZE):
            for chunk in batch:
                yield Task[Chunk](
                    id=chunk.chunk_id,
                    name=str(chunk.chunk_id),
                    entity=chunk,
                    routes=[Action.GRAB],
                )

    async def process(self, batch: List[Task[Chunk]], wid: int) -> List[Task[Chunk]]:
        return batch

    async def count_total(self) -> None:
        total = self._repo.count()
        await self.stats.set_total(self.name, total)