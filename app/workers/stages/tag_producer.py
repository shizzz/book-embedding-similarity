from typing import List
from app.common.types import TEntity
from app.workers.base import BaseQueueWorker
from app.infrastructure.db.repositories import GenresRepository, CentroidsRepository
from app.infrastructure.models import Task, Tag, Stages, Action, Chunk, ChunkType, Embedding, Dataset, Channel

ROUTES = {
    Action.TOKENIZE: {Stages.TOKENIZER},
    Action.TAG: {Stages.TAG},
}

class TagProducer(BaseQueueWorker):
    """
    Читает теги из источника тегов
    """
    def __init__(
            self, 
            repo: GenresRepository | CentroidsRepository,
            type: ChunkType,
            name: str = Stages.PRODUCER + "_tag",
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)

        self._repo = repo
        self._type = type
        if isinstance(repo, GenresRepository):
            self._action = Action.TOKENIZE
        else:
            self._action = Action.TAG

    async def produce(self):
        for tag in self._repo.get_all(self._type):
            yield Task[Tag](
                id=tag.id,
                name=tag.name_ru,
                entity=tag,
                routes=self._action,
            )

    async def process(self, batch: List[Task[Tag]], wid: int) -> List[Task[TEntity]]:
        result = []

        for b in batch:
            if Action.TOKENIZE in b.routes:
                result.append(
                    Task(
                        id=b.entity.id,
                        name=b.entity.name_ru,
                        cost=len(b.entity.name_ru) * 2,
                        entity=Chunk(
                            source_id=b.entity.id,
                            chunk_id=b.entity.name_ru,
                            text=b.entity.name_ru,
                            type=ChunkType.TAG
                        ),
                        routes=b.routes,
                        dataset=Dataset.TAG,
                    )
                )
            else:
                dataset = Dataset.TAG if b.entity.type == ChunkType.TAG else Dataset.CENROID
                result.append(
                    Task(
                        id=b.entity.id,
                        name=b.entity.name_ru,
                        cost=len(b.entity.name_ru) * 2,
                        entity=Embedding(
                            id=b.entity.id,
                            data=b.entity.data,
                            shape=len(b.entity.data),
                            source_id=b.entity.parent_id,
                            type=b.entity.type
                        ),
                        routes=b.routes,
                        dataset=dataset,
                    )
                )

        return result

    async def count_total(self) -> None:
        total = self._repo.count()
        await self.stats.set_total(self.name, total)

    def route(self, task: Task, channels: list[Channel]) -> list[Channel]:
        allowed = set()
        for action in task.routes:
            allowed |= ROUTES.get(action, set())

        return [ch for ch in channels if ch.downstream in allowed]