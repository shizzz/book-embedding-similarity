from app.workers import BaseWorker
from app.services.similar_search_service import SimilarSearchService
from app.models import Task, BookRegistry

class GenerateSimilarWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def stat_books(self):
        books = await self.db.load_books_with_embeddings()
        await self.registry.fill_from_books(books, True)

        self.embeddings = BookRegistry(books)

    def process_book(self, task: Task):
        service = SimilarSearchService(
            source=task.book,
            registry=self.embeddings,
            limit=100,
            exclude_same_authors=False,
            step_percent=5)
        
        similar = service.run()
        self.db.save_similar(task.book.file_name, similar)