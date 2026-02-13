import time
from typing import Any, Tuple
from app.workers import BaseWorker
from app.utils import FB2Book
from app.hnsw import HNSW
from app.models import Task, Embedding, Book
from app.db import db, BookRepository, EmbeddingsRepository, AuthorRepository, FeedbackRepository
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.settings.config import INPX_FOLDER

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model
        self.hnsw = HNSW(batch_size=10000)
        self.engine = BookSearchEngineFactory.create(BookSearchEngineFactory.INPIX, INPX_FOLDER)

    async def stat_books(self):
        return True

    async def pull_queue(self):
        last_update = 0
        
        async for book in self.engine.search_books():
            await self.registry.add_one(Task(
                name=book.file_name,
                book=book
            ))

            now = time.time()
            if now - last_update >= 1:
                await self.ui.update_total(self.registry.total)
                last_update = now
                
        await self.ui.update_total(self.registry.total)
        self._queue_pulled = True

    def process_book(self, task: Task):
        data = task.book.get_file_bytes_from_zip()
        book = FB2Book(data)

        text = book.extract_text()
        id = book.get_id()

        title = task.book.title or book.get_title()
        authors = task.book.authors or book.get_authors()
        author = task.book.author or ", ".join(authors)

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
            embeddings = list[Tuple[int, bytes]](EmbeddingsRepository().get_all(conn))
            feedbacks = FeedbackRepository().get_all(conn)
            books: list[Book] = [
                Book.map_row(row)
                for row in BookRepository().get_all(conn)
            ]
            
        self.hnsw.load_emb(embeddings)
        self.hnsw.rebuild(
            feedbacks=feedbacks,
            books=books,
        )