import asyncio
from asyncio import to_thread
from typing import Tuple
from app.workers import BaseWorker
from app.utils import FB2Book
from app.hnsw import HNSW
from app.models import Embedding, Book, Feedbacks, Task
from app.db import db, BookRepository, EmbeddingsRepository, AuthorRepository, FeedbackRepository
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.settings.config import INPX_FOLDER

class GenerateEmbeddingsWorker(BaseWorker):
    BOOK_BATCH_SIZE: int = 10

    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model
        self.hnsw = HNSW(batch_size=10000)
        self.engine = BookSearchEngineFactory.create(BookSearchEngineFactory.INPIX, INPX_FOLDER)
        self._get_book_idx: int = None
        self._book_id: int = 1
        self._db_queue = asyncio.Queue()

    async def stat_books(self):
        with db() as conn:
            self._book_id = BookRepository.get_max_id(conn)
        return True
    
    async def get_total(self) -> int:
        total = await self.engine.get_total()
        await self.ui.update_total(total, self._get_book_idx)
        return total / self.BOOK_BATCH_SIZE

    async def pull_queue(self):
        buffer = []
        self._get_book_idx = self.ui.add_progress("Парсинг книг ", "обработано")
        async for book_task in self.engine.search_books():    
            book = await self.engine.get_book(book_task)
            book.id = self._book_id
            self._book_id += 1

            buffer.append(book)
            await self.ui.done(self._get_book_idx)

            if len(buffer) >= self.BOOK_BATCH_SIZE:                
                await self.queue.put(Task(book_task.link, buffer.copy()))
                buffer.clear()
            
        if len(buffer) > 0:                
            await self.queue.put(Task(book_task.link, buffer))
        self._queue_pulled = True

    async def process_book(self, task: Task):
        await to_thread(self._process_book, task.entity)

    async def fin(self):
        with db() as conn:
            embeddings = list[Tuple[int, bytes]](EmbeddingsRepository.get_all(conn))
            feedbacks = Feedbacks(FeedbackRepository.get_all(conn))
            books: list[Book] = [
                Book.map_row(row)
                for row in BookRepository.get_all(conn)
            ]
            
        self.hnsw.load_emb(embeddings)
        self.hnsw.rebuild(
            feedbacks=feedbacks,
            books=books,
        )

    def _process_book(self, books: list[Book]):
        texts = []

        # parse
        for book in books:
            fb2 = FB2Book(book.data)
            fb2.enrich_book(book)
            texts.append(fb2.extract_text())

        embeddings_np = self.model.encode(
            texts,
            batch_size=128,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        embeddings_db = [
            Embedding(vec).to_db()
            for vec in embeddings_np
        ]

        with db() as conn:
            BookRepository.save_bulk(conn, books)
            EmbeddingsRepository.save_bulk(conn, books, embeddings_db)
            AuthorRepository.save_bulk(conn, books)
