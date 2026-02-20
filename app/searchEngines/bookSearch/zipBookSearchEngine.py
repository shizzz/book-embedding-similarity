import asyncio
from typing import AsyncGenerator
from app.db import db, BookRepository
from app.models import Book
from app.utils import FB2Book
from .bookSearchEngine import BaseBookSearchEngine
from app.searchEngines.sources import RemoteBookScanner


class ZipBookSearchEngine(BaseBookSearchEngine):
    TYPE: str = "zip"

    def __init__(self, folder: str):
        self.folder = folder
        self._completed_books: set[str] = set()
        self._completed_books_loaded = asyncio.Event()

    # -----------------------------
    # Загрузка списка уже обработанных книг
    # -----------------------------
    def _load_completed_books(self):
        try:
            with db() as conn:
                self._completed_books = set(BookRepository.get_names(conn))
        finally:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(self._completed_books_loaded.set)

    # -----------------------------
    # Поиск книг
    # -----------------------------
    async def search_books(self) -> AsyncGenerator[Book, None]:
        await self._completed_books_loaded.wait()

        with RemoteBookScanner(self.folder, self._completed_books) as scanner:
            archives = await asyncio.to_thread(scanner.list_archives)

            for archive in archives:
                books = await scanner.scan_archive(archive)
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
        await self._load_completed_books()
        await self._completed_books_loaded.wait()
        total = 0

        with RemoteBookScanner(self.folder, self._completed_books) as scanner:
            archives = await asyncio.to_thread(scanner.list_archives)

            for archive in archives:
                total += await scanner.archive_book_total(archive)

        return total

    # -----------------------------
    # Получение данных книги
    # -----------------------------
    async def enrich_book_data(self, book: Book):
        # Разделяем archive_name и file_name из ссылки
        archive_name, file_name = book.source_link.split("/", 1)

        with RemoteBookScanner(self.folder, self._completed_books) as scanner:
            book.data = await scanner.get_book_data(archive_name, file_name)
            fb2 = FB2Book(book.data)
            fb2.enrich_book(book)
