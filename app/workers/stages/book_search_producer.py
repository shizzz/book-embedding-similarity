import asyncio
from typing import List
from app.searchEngines.bookSearch import BaseBookSearchEngine
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookRepository
from app.infrastructure.models import Task, Book, BookAction, BookTask, Stages

class BookProducer(BaseQueueWorker[BookTask]):
    """
    Читает книги из источника библиотеки
    """
    def __init__(
            self, 
            router: DBRouter,
            search_engine: BaseBookSearchEngine,
            name: str = Stages.BOOK_SEARCH,
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)

        self._engine = search_engine
        self._repo = BookRepository(router)
        self._book_id = BookRepository(router).get_max_id()
        self._batch: List[Task[BookTask]] = []

    def has_input(self) -> bool:
        return False

    async def produce(self):
        file_to_id = self._repo.get_file_to_id()     
        self.count_task = asyncio.create_task(self.count_total())

        async for book in self._engine.search_books():
            if book.file_name in file_to_id:
                book = await self._enrich_from_db(book)
                data = None
                if not book.empty and len(book.chunks or []) == 0:
                    data = await self._engine.get_book_data(book)
                
                yield Task(
                    id=book.id,
                    name=book.file_name,
                    entity=BookTask(book, data, BookAction.CHUNK)
                )
            else:
                book.id = self.reserve_id()
                data = await self._engine.get_book_data(book)

                if not data:
                    continue

                yield Task(
                    id=book.id,
                    name=book.file_name,
                    entity=BookTask(book, data, BookAction.BOOK)
                )

    async def _enrich_from_db(self, book: Book) -> Book:
        def _sync(file_name: str):
            db_book = self._repo.get_full_by_file(file_name)
            if not db_book:
                return None

            return db_book

        db_book = await asyncio.to_thread(_sync, book.file_name)

        return book.merge_from(db_book)

    async def count_total(self) -> None:
        total = await self._engine.get_total()
        await self.stats.set_total(self.name, total)
    
    def reserve_id(self) -> int:
        id = self._book_id
        self._book_id += 1
        return id