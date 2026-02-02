from app.workers import BaseWorker
from app.utils import FB2Book
from app.models import Task

class GenerateAuthorsWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def stat_books(self):
        tasks = await self.db.load_books_with_authors()
        await self.registry.fill_from_books(tasks)
        s = 1

    def process_book(self, task: Task):
        data = task.book.get_file_bytes_from_zip()
        book = FB2Book(data)
        authors = book.get_authors()
    
        self.db.update_book_authors(
            book=task.book.file_name,
            authors=authors
        )