import asyncio
from abc import ABC, abstractmethod
from typing import AsyncGenerator
from app.workers.stats import Stats
from app.infrastructure.models import Book
from app.searchEngines.sources import BookSourceManager

class BaseBookSearchEngine(ABC):
    def __init__(self, folder: str, stats: Stats = None):
        self._manager = BookSourceManager(
            folder=folder,
            stats=stats
        )
        self._semaphore = asyncio.Semaphore(3)
        self._tasks = []

    @abstractmethod
    async def search_books(self) -> AsyncGenerator[Book, None]:
        if False:
            yield

    @abstractmethod
    async def get_book_data(self, book: Book) -> bytes:
        pass

    @abstractmethod
    async def get_total(self) -> int:
        pass

    async def fetch_with_semaphore(self, archive_name):
        async with self._semaphore:
            await self._manager.ensure_archive_in_cache(archive_name)