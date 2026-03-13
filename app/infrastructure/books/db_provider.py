from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookRepository
from app.infrastructure.models import Book
from app.infrastructure.providers.bookProvider import BookProvider

class DBBookProvider(BookProvider):
    SQLITE_MAX_VARS = 30000

    def __init__(self, router: DBRouter):
        self._repo = BookRepository(router)

    def _chunks(self, items: list[int], size: int):
        for i in range(0, len(items), size):
            yield items[i:i + size]

    def get_by_id(self, book_id: int) -> Book:
        return self._repo.get_by_id(book_id)

    def get_many(self, book_ids: list[int]) -> dict[int, Book]:
        result: dict[int, Book] = {}

        for chunk in self._chunks(book_ids, self.SQLITE_MAX_VARS):
            result.update(self._repo.get_many(chunk))

        return result