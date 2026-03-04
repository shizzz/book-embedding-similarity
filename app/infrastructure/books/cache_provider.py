from app.infrastructure.models import Book
from app.infrastructure.providers.bookProvider import BookProvider

class CacheBookProvider(BookProvider):
    def __init__(self, books: dict[int, Book]):
        self._books = books

    def get_by_id(self, book_id: int):
        return self._books.get(book_id)

    def get_many(self, book_ids: list[int]):
        return {
            book_id: self._books[book_id]
            for book_id in book_ids
            if book_id in self._books
        }