import asyncio
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Any
from app.infrastructure.models import Book
from app.searchEngines.sources import BookSourceManager

class BaseBookSearchEngine(ABC):
    def __init__(self, folder: str, ui: Any = None):
        self._manager = BookSourceManager(
            folder=folder,
            ui=ui
        )
        self._semaphore = asyncio.Semaphore(3)
        self._tasks = []

    @abstractmethod
    async def search_books(self) -> AsyncGenerator[Book, None]:
        if False:
            yield  # чтобы типизатор понял что это async generator

    @abstractmethod
    async def enrich_book_data(self, book: Book) -> Book:
        pass

    @abstractmethod
    async def get_book_data(self, book: Book) -> bytes:
        pass

    @abstractmethod
    async def get_total(self) -> int:
        pass

    async def fetch_with_semaphore(self, archive_name):
        async with self._semaphore:
            await self._manager.ensure_archive_in_cache(archive_name)