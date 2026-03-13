from typing import AsyncGenerator
from app.infrastructure.db.repositories import BookRepository
from app.infrastructure.db.iterables import BookBatchIterable
from app.infrastructure.db import DBRouter
from app.workers.stats import Stats
from app.infrastructure.models import Book
from .bookSearchEngine import BaseBookSearchEngine

BATCH_SIZE: int = 100

class DbpBookSearchEngine(BaseBookSearchEngine):
    def __init__(
            self, 
            folder: str,
            stats: Stats = None,
            router: DBRouter = None
        ):
        super().__init__(folder, stats)
        repo = BookRepository(router)
        self._iter = BookBatchIterable(
            repo=repo,
            batch_size=BATCH_SIZE,
            empty=False,
        )

    # -----------------------------
    # Поиск книг
    # -----------------------------
    async def search_books(self) -> AsyncGenerator[Book, None]:
        for books in self._iter:
            for book in books:
                yield book

    # -----------------------------
    # Подсчет общего количества книг
    # -----------------------------
    async def get_total(self) -> int:
        return self._iter.count()

    # -------------------------------------
    # Из базы данных не вынуть данные книги
    # -------------------------------------
    async def get_book_data(self, book: Book) -> bytes:
        return None