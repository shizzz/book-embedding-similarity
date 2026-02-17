from abc import ABC, abstractmethod
from typing import AsyncGenerator
from app.models import Book, BookResult

class BaseBookSearchEngine(ABC):
    @abstractmethod
    async def search_books(self) -> AsyncGenerator[BookResult, None]:
        pass

    @abstractmethod
    async def get_book(self, bookResult: BookResult) -> Book:
        pass

    @abstractmethod
    async def get_total(self) -> int:
        pass