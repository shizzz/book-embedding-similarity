from abc import ABC, abstractmethod
from typing import AsyncGenerator
from app.models import Book

class BaseBookSearchEngine(ABC):
    @abstractmethod
    async def search_books(self) -> AsyncGenerator[Book, None]:
        pass

    @abstractmethod
    async def enrich_book_data(self, book: Book) -> Book:
        pass

    @abstractmethod
    async def get_total(self) -> int:
        pass