import os
import zipfile
from app.workers import BaseWorker
from app.utils import FB2Book
from app.models import Book, Task
from app.services import HNSWService
from app.settings.config import BOOK_FOLDER

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    async def stat_books(self):
        completed_books = set(await self.db.load_books_only())
        tasks = []

        for archive in os.listdir(BOOK_FOLDER):
            if not archive.lower().endswith(".zip"):
                continue
            
            with zipfile.ZipFile(os.path.join(BOOK_FOLDER, archive)) as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue
                    if info.filename in completed_books:
                        continue
                    
                    tasks.append(Task(
                        name=info.filename,
                        book=Book(
                            archive_name=archive,
                            file_name=info.filename
                        )
                    ))
        await self.registry.add(tasks)
        hnswService = HNSWService()

        # После обновления\добавления эмбедингов, индекс нужно перестроить
        hnswService.delete_index_file()

    def process_book(self, task: Task):
        data = task.book.get_file_bytes_from_zip()
        book = FB2Book(data)

        text = book.extract_text()
        id = book.get_id()
        authors = book.get_authors()
        author = ", ".join(authors)
        title = book.get_title()

        embedding = self.model.encode(text)

        self.db.save_book(
            task.book.file_name,
            task.book.archive_name,
            id,
            title,
            author,
            embedding)
        
        self.db.update_book_authors(
            book=task.book.file_name,
            authors=authors
        )
