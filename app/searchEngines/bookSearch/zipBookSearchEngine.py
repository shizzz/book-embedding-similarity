import asyncio
from typing import AsyncGenerator
from app.workers.stats import Stats
from app.infrastructure.models import Book, BookSearchEngineType
from .bookSearchEngine import BaseBookSearchEngine


class ZipBookSearchEngine(BaseBookSearchEngine):
    def __init__(self, folder: str, stats: Stats = None):
        super().__init__(folder, stats)

    # -----------------------------
    # Поиск книг
    # -----------------------------
    async def search_books(self) -> AsyncGenerator[Book, None]:
        archives = await asyncio.to_thread(self._manager.list_archives)

        if self._manager.is_remote:
            for archive in archives:
                task = asyncio.create_task(self.fetch_with_semaphore(archive))
                self._tasks.append(task)

        for archive in archives:
            books = await self._manager.scan_archive(archive)
            for file_name, archive_name in books:
                yield Book(
                    file_name=file_name,
                    source_type=BookSearchEngineType.ZIP,
                    source_link=f"{archive_name}/{file_name}"
                )

    # -----------------------------
    # Подсчет общего количества книг
    # -----------------------------
    async def get_total(self) -> int:
        total = 0

        archives = await asyncio.to_thread(self._manager.list_archives)

        for archive in archives:
            total += await self._manager.archive_book_total(archive)

        return total
