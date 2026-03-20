from typing import List, Dict
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookRepository, BookTagsRepository
from app.infrastructure.models import Book
from app.infrastructure.providers.bookProvider import BookProvider

class DBBookProvider(BookProvider):
    SQLITE_MAX_VARS = 30000

    def __init__(self, router: DBRouter):
        self._repo = BookRepository(router)
        self._tags_repo = BookTagsRepository(router, BookTagsRepository.GENRES_TABLE)
        self._centroids_repo = BookTagsRepository(router, BookTagsRepository.CENTOIDS_TABLE)

    def _chunks(self, items: List[int], size: int):
        for i in range(0, len(items), size):
            yield items[i:i + size]

    def get_by_id(self, book_id: int) -> Book:
        book = self._repo.get_by_id(book_id)
        if book:
            book.tags = self._tags_repo.get_by_book(book_id)
            book.centroids = self._centroids_repo.get_by_book(book_id)
        return book

    def get_many(self, book_ids: List[int]) -> Dict[int, Book]:
        """
        Получение нескольких книг с тегами и центроидами.
        Возвращает словарь {book_id: Book}.
        """
        result: Dict[int, Book] = {}

        for chunk in self._chunks(book_ids, self.SQLITE_MAX_VARS):
            # Получаем книги
            books_chunk: Dict[int, Book] = self._repo.get_many(chunk)

            # Если нет книг в чанке, пропускаем
            if not books_chunk:
                continue

            ids_in_chunk = list(books_chunk.keys())

            # Получаем все теги для чанка
            tags_by_book: Dict[int, List] = {book_id: [] for book_id in ids_in_chunk}
            centroids_by_book: Dict[int, List] = {book_id: [] for book_id in ids_in_chunk}

            # массовое получение тегов
            for book_id, tags in self._tags_repo.get_many(ids_in_chunk).items():
                tags_by_book[book_id] = tags

            # массовое получение центроидов
            for book_id, centroids in self._centroids_repo.get_many(ids_in_chunk).items():
                centroids_by_book[book_id] = centroids

            # объединяем все в объекты Book
            for book_id, book in books_chunk.items():
                book.tags = tags_by_book.get(book_id, [])
                book.centroids = centroids_by_book.get(book_id, [])

            # добавляем в общий результат
            result.update(books_chunk)

        return result