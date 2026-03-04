import asyncio
from typing import AsyncGenerator, Any
from app.infrastructure.models import Book
from app.utils import FB2Book
from .bookSearchEngine import BaseBookSearchEngine


class ZipBookSearchEngine(BaseBookSearchEngine):
    TYPE: str = "zip"

    def __init__(self, folder: str, ui: Any = None):
        super().__init__(folder, ui)

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
                    source_type=self.TYPE,
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

    # -----------------------------
    # Получение данных книги
    # -----------------------------
    async def enrich_book_data(self, book: Book):
        archive_name, file_name = book.source_link.split("/", 1)
        data = await self._manager.get_book_data(archive_name, file_name)
        book.source_length = len(data)
        fb2 = FB2Book(data)
        fb2.enrich_book(book)
