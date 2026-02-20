import asyncio
from asyncio import to_thread
from typing import Tuple
from app.workers import BaseWorker
from app.hnsw import IndexManager
from app.model import Model, generate_embeddings
from app.db import db, BookRepository, EmbeddingsRepository, AuthorRepository, FeedbackRepository
from app.models import Book, BookRegistry, Feedbacks, Task, TaskResult, Action
from app.common.types import TEntity
from app.searchEngines.bookSearch import BookSearchEngineFactory

class GenerateEmbeddingsWorker(BaseWorker):
    MAX_BOOK_BATCH_SIZE: int = 500

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.hnsw = IndexManager(batch_size=10000)
        self.engine = BookSearchEngineFactory.create(BookSearchEngineFactory.INPIX)
        self._get_book_idx: int = None
        self._book_id: int = 1
        self._db_queue_batch_size = 100

        self._model = Model()

    async def process(self, task: Task) -> TaskResult:
        result = await to_thread(self._process_book, task.entity)
        return task.to_result(
            len(task.entity),
            result
        )
    
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
        registry = BookRegistry()
        async for book in self.engine.search_books():         
            if self._cancellation_token.is_set():
                return
        
            await self.engine.enrich_book_data(book)
            book.id = self._book_id
            self._book_id += 1

            registry.append(book)
            await self.ui.done_async(self._get_book_idx)

            batch_size = self._adaptive_batch_size(self.queue.qsize() + len(registry))
            if len(registry) >= batch_size:                
                await self.queue.put(
                    Task(
                            name=f"{book.source_link} ({batch_size})", 
                            entity=registry.copy(),
                            action=Action.INSERT
                        )
                    )
                registry.clear()
            
        if len(registry) > 0:                
            await self.queue.put(
                Task(
                        name=book.source_link, 
                        entity=registry,
                        action=Action.INSERT
                    )
                )
        await self.enqueue_shutdown_signals_async()
        self._queue_pulled = True

    def save_to_db(self, conn, task: Task) -> int:
        BookRepository.save_bulk(conn, task.entity)
        EmbeddingsRepository.save_bulk(conn, task.entity)
        AuthorRepository.save_bulk(conn, task.entity)
        return len(task.entity)

    def _process_book(self, registry: BookRegistry) -> BookRegistry:
        return generate_embeddings(self._model, registry)

    def _adaptive_batch_size(self, queue_size: int,) -> int:
        """
        Вычисляет адаптивный размер пакета для очереди.
        - queue_size: текущее количество элементов в очереди
        - max_batch: максимальный размер пакета
        """
        if queue_size < 10:
            # Если мало элементов, возвращаем число меньше 10
            return 5
        
        # Для больших чисел: округляем до ближайшего "красивого" числа
        # Красивое число — кратное 10, не больше max_batch
        batch = min(queue_size, self.MAX_BOOK_BATCH_SIZE)
        # Округление вниз до ближайшего кратного 10
        batch = (batch // 10) * 10
        return max(10, batch)