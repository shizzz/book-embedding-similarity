from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task


class GenerateSimilarWorker(BaseWorker):
    __service: BulkSimilarSearchService

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def stat_books(self):
        self.__service = BulkSimilarSearchService(
            limit=100,
            exclude_same_authors=False
        )
        await self.registry.fill_from_books(self.__service.valid_books, True)

    def process_book(self, task: Task):
        similar = self.__service.run(task.book)
        self.db.save_similar(task.book.file_name, similar)