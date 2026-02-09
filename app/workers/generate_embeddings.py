import os
import zipfile
from tqdm import tqdm
from app.workers import BaseWorker
from app.utils import FB2Book
from app.models import Book, Task, Embedding
from app.services import HNSWService
from app.db import db, BookRepository, EmbeddingsRepository, AuthorRepository
from app.settings.config import BOOK_FOLDER

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model
        self.hnswService = HNSWService(batch_size=10000)

    async def stat_books(self):
        with db() as conn:
            completed_books = set(BookRepository.get_names(conn))
        tasks = []
  
        with tqdm(total=len(os.listdir(BOOK_FOLDER)), desc="Проверка архивов", unit=" с", unit_scale=True) as pbar:
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
                pbar.update(1)
        await self.registry.add(tasks)

        # После обновления\добавления эмбедингов, индекс нужно перестроить
        self.hnswService.delete_index_file()

    def process_book(self, task: Task):
        data = task.book.get_file_bytes_from_zip()
        book = FB2Book(data)

        text = book.extract_text()
        id = book.get_id()
        authors = book.get_authors()
        author = ", ".join(authors)
        title = book.get_title()

        embedding = self.model.encode(text)

        with db() as conn:
            book_id = BookRepository.save(
                conn=conn,
                book=task.book.file_name,
                archive=task.book.archive_name,
                uid=id,
                title=title,
                author=author)
            
            EmbeddingsRepository.save(
                conn=conn,
                book_id=book_id,
                embedding=Embedding(embedding).to_db()
            )
            
            AuthorRepository.save(
                conn=conn, 
                book_id=book_id, 
                authors=authors)

    async def fin(self):
        with db() as conn:
            embeddings = list(EmbeddingsRepository().get_all(conn))
            
        self.hnswService.load_emb(embeddings)
        self.hnswService.generate_and_save()