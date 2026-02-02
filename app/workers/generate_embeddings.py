import os
import zipfile
from app.workers import BaseWorker
from app.utils import FB2Book
from app.models import BookTask
from app.settings.config import BOOK_FOLDER

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    def stat_books(self):
        completed_books = self.db.load_books_only()

        for archive in os.listdir(BOOK_FOLDER):
            if not archive.lower().endswith(".zip"):
                continue

            with zipfile.ZipFile(os.path.join(BOOK_FOLDER, archive)) as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue

                    completed = (archive, info.filename) in completed_books

                    self.registry.add_book(
                        archive_name=archive,
                        file_name=info.filename,
                        completed=completed
                    )

    def process_book(self, task: BookTask):
        data = task.get_file_bytes_from_zip()
        book = FB2Book(data)

        text = book.extract_text()
        id = book.get_id()
        authors = book.get_authors()
        author = ", ".join(authors)
        title = book.get_title()

        embedding = self.model.encode(text)

        self.db.save_book_with_emb(
            task.file_name,
            task.archive_name,
            id,
            title,
            author,
            embedding)
        
        self.db.update_book_authors(
            book=task.file_name,
            authors=authors
        )
