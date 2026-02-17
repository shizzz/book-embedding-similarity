from asyncio import to_thread
from typing import Tuple, List
from app.workers import BaseWorker
from app.utils import FB2Book
from app.hnsw import HNSW
from app.model import Model
from app.models import Embedding, Book, Feedbacks, Task
from app.db import db, BookRepository, EmbeddingsRepository, AuthorRepository, FeedbackRepository
from app.searchEngines.bookSearch import BookSearchEngineFactory

class GenerateEmbeddingsWorker(BaseWorker):
    MAX_BOOK_BATCH_SIZE: int = 10000

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.hnsw = HNSW(batch_size=10000)
        self.engine = BookSearchEngineFactory.create(BookSearchEngineFactory.INPIX)
        self._get_book_idx: int = None
        self._book_id: int = 1
        self._db_queue_batch_size = 100

        self._model = Model()
        self._model_id = self._model.get_model_uid()
        self._transformer = self._model.get()

    async def process(self, task: Task) -> Tuple[int, Tuple[List[Book], List[bytes]]]:
        return (len(task.entity), await to_thread(self._process_book, task.entity))
    
    async def prepare(self) -> None:
        self._get_book_idx = self.ui.add_progress("Парсинг книг", "книг")
        with db() as conn:
            self._book_id = BookRepository.get_max_id(conn)
    
    async def get_total(self) -> int:
        total = await self.engine.get_total()
        await self.ui.update_total(total, self._get_book_idx)
        return total

    async def fin(self) -> None:
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

    async def pull_queue(self) -> None:
        buffer = []
        async for book in self.engine.search_books():    
            await self.engine.enrich_book_data(book)
            book.id = self._book_id
            self._book_id += 1

            buffer.append(book)
            await self.ui.done(self._get_book_idx)

            batch_size = self._adaptive_batch_size(self.queue.qsize() + len(buffer))
            if len(buffer) >= batch_size:                
                await self.queue.put(Task(f"{book.source_link} ({batch_size})", buffer.copy()))
                buffer.clear()
            
        if len(buffer) > 0:                
            await self.queue.put(Task(book.source_link, buffer))
        self._queue_pulled = True

    def save_to_db(self, buffer: List[Book]) -> int:
        with db() as conn:
            BookRepository.save_bulk(conn, buffer)
            EmbeddingsRepository.save_bulk(conn, buffer)
            AuthorRepository.save_bulk(conn, buffer)
        return len(buffer)

    def _process_book(self, books: list[Book]) -> List[Book]:
        texts = []

        for book in books:
            fb2 = FB2Book(book.data)
            fb2.enrich_book(book)
            texts.append(fb2.extract_text())

        embeddings_np = self._transformer.encode(
            texts,
            batch_size=128,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        for book, vec in zip(books, embeddings_np):
            book.embedding = Embedding(vec)
            book.model_id = self._model_id

        return books

    def _adaptive_batch_size(self, queue_size: int,) -> int:
        """
        Вычисляет адаптивный размер пакета для очереди.
        - queue_size: текущее количество элементов в очереди
        - max_batch: максимальный размер пакета
        """
        if queue_size < 10:
            # Если мало элементов, возвращаем число меньше 10
            return max(1, queue_size)
        
        # Для больших чисел: округляем до ближайшего "красивого" числа
        # Красивое число — кратное 10, не больше max_batch
        batch = min(queue_size, self.MAX_BOOK_BATCH_SIZE)
        # Округление вниз до ближайшего кратного 10
        batch = (batch // 10) * 10
        return max(10, batch)