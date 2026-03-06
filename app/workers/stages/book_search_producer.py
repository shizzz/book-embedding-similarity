import asyncio
from typing import List
from app.searchEngines.bookSearch import BaseBookSearchEngine
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookRepository
from app.infrastructure.models import Task, TaskResult, Book, BookAction, BookTask

class BookProducer(BaseQueueWorker[BookTask]):
    """
    Читает книги из источника библиотеки
    """
    def __init__(
            self, 
            router: DBRouter,
            search_engine: BaseBookSearchEngine,
            *args, 
            **kwargs
        ):
        super().__init__(name="BookProducer", *args, **kwargs)

        self._engine = search_engine
        self._repo = BookRepository(router)
        self._book_id = BookRepository(router).get_max_id()
        self._batch: List[TaskResult[BookTask]] = []

    async def produce(self):
        file_to_id = self._repo.get_file_to_id()
        async for book in self._engine.search_books():
            if book.file_name in file_to_id:
                book.id = self.reserve_id()
                data = await self._engine.get_book_data(book)

                if not data:
                    continue

                yield Task(
                    task_id=book.id,
                    name=book.file_name,
                    entity=BookTask(book, data, BookAction.CHUNK_FROM_BOOK)
                )
            else:
                book = await self._enrich_from_db(book)
                data = None
                if not book.empty and len(book.chunks or []) == 0:
                    data = await self._engine.get_book_data(book)

                yield Task(
                    task_id=book.id,
                    name=book.file_name,
                    entity=BookTask(book, data, BookAction.CHUNK_FROM_DB)
                )

    async def _enrich_from_db(self, book: Book) -> Book:
        def _sync(file_name: str):
            db_book = BookRepository(self._router).get_full_by_file(file_name)
            if not db_book:
                return None

            return db_book

        db_book = await asyncio.to_thread(_sync, book.file_name)

        return book.merge_from(db_book)

    def reserve_id(self) -> int:
        id = self._book_id
        self._book_id += 1
        return id