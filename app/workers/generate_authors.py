from app.workers import BaseWorker
from app.utils import FB2Book
from app.models import BookTask

class GenerateAuthorsWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def stat_books(self):
        rows = self.db.load_books_with_authors()
        self.registry.bulk_add_from_db(rows)

    def process_book(self, task: BookTask):
        data = task.get_file_bytes_from_zip()
        book = FB2Book(data)
        authors = book.get_authors()
    
        self.db.update_book_authors(
            book=task.file_name,
            authors=authors
        )