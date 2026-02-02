from app.workers import BaseWorker
from app.services.similar_search_service import SimilarSearchService
from app.models import Task, BookRegistry

class SimilarProcessQueueWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def stat_books(self):
        books = await self.db.load_books_with_embeddings()

        self.embeddings = BookRegistry()
        self.embeddings.add_books(books)

        tasks = self.db.get_queue()
        await self.registry.fill_from_queue(tasks)

    def process_book(self, task: Task):
        book = self.embeddings.get_book_by_name(task.queueRecord.book)
        
        service = SimilarSearchService(
            source=book,
            registry=self.embeddings,
            limit=task.queueRecord.book_count,
            exclude_same_authors=task.queueRecord.exclude_same_author,
            step_percent=5,
            queue = task.queueRecord)
        service.run()
        similar = service.get_result()
        
        self.db.save_similar(book.file_name, similar)
        self.db.dequeue_process(task.queueRecord.book)