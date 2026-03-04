from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookRepository
from app.infrastructure.providers.bookProvider import BookProvider

class DBBookProvider(BookProvider):
    def __init__(self, router: DBRouter):
        self._repo = BookRepository(router)

    def get_by_id(self, book_id: int):
        return self._repo.get_by_id(book_id)

    def get_many(self, book_ids: list[int]):
        return self._repo.get_many(book_ids)