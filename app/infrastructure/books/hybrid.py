from app.infrastructure.db import DBRouter
from app.infrastructure.models import Book
from app.infrastructure.providers.bookProvider import BookProvider
from .db_provider import DBBookProvider

class HybridBookProvider(BookProvider):
    def __init__(
            self, 
            router: DBRouter
        ):
        self._db = DBBookProvider(router)
        self._cache: dict[int, Book] = {}

    def get_by_id(self, book_id: int):
        if book_id in self._cache:
            return self._cache[book_id]

        book = self._db.get_by_id(book_id)
        if book:
            self._cache[book_id] = book
        return book

    def get_many(self, book_ids: list[int]):
        result = {}
        missing = []

        for book_id in book_ids:
            if book_id in self._cache:
                result[book_id] = self._cache[book_id]
            else:
                missing.append(book_id)

        if missing:
            fetched = self._db.get_many(missing)
            result.update(fetched)
            self._cache.update(fetched)

        return result