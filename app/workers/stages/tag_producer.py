from typing import List
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Tag, Stages, Action

class TagProducer(BaseQueueWorker):
    """
    Читает теги из источника тегов
    """
    def __init__(
            self, 
            repo,
            name: str = Stages.TAG,
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)

        self._repo = repo

    async def produce(self):
        for tag in self._repo.get_all():
            yield Task[Tag](
                id=tag.id,
                name=tag.name_ru,
                entity=tag,
                routes=Action.GRAB,
            )

    async def process(self, batch: List[Task[Tag]], wid: int) -> List[Task[Tag]]:
        return batch

    async def count_total(self) -> None:
        total = await self._repo.get_total()
        await self.stats.set_total(self.name, total)