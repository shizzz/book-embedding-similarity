import os
import asyncio
import zipfile
from typing import AsyncGenerator
from app.db import db, BookRepository
from app.models import Book
from app.utils import get_file_bytes_from_zip
from .bookSearchEngine import BaseBookSearchEngine

class ZipBookSearchEngine(BaseBookSearchEngine):
    TYPE: str = "zip"

    def __init__(self, folder: str):
        self.folder = folder
        self._completed_books: set[str] = set()
        self._completed_books_loaded = asyncio.Event()

    def _load_completed_books(self) -> set[str]:
        try:
            with db() as conn:
                self._completed_books = set[str](BookRepository.get_names(conn))
        finally:
            # сигнализируем async-коду, что загрузка завершена
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(self._completed_books_loaded.set)

    def _list_archives(self) -> list[str]:
        return [
            f for f in os.listdir(self.folder)
            if f.lower().endswith(".zip")
        ]

    def _archive_book_total(self, archive: str, completed_books: set[str]) -> int:
        archive_path = os.path.join(self.folder, archive)
        result = 0

        with zipfile.ZipFile(archive_path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                if info.filename in completed_books:
                    continue

                result += 1

        return result

    def _scan_archive(self, archive: str, completed_books: set[str]) -> list[Book]:
        archive_path = os.path.join(self.folder, archive)
        result = []

        with zipfile.ZipFile(archive_path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                if info.filename in completed_books:
                    continue
                
                link = f"{archive}/{info.filename}"

                result.append(
                        Book(
                            file_name=info.filename,
                            source_type=self.TYPE,
                            source_link=link
                        )
                )

        return result
        
    async def search_books(self) -> AsyncGenerator[Book, None]:
        await self._completed_books_loaded.wait()
        archives = await asyncio.to_thread(self._list_archives)

        for archive in archives:
            books = await asyncio.to_thread(
                self._scan_archive,
                archive,
                self.completed
            )

            for book in books:
                yield book

    async def get_total(self) -> int:
        await asyncio.to_thread(self._load_completed_books)
        
        total = 0
        archives = await asyncio.to_thread(self._list_archives)
        for archive in archives:
            total += await asyncio.to_thread(
                    self._archive_book_total,
                    archive,
                    self.completed
                )
            
        return total
    
    async def enrich_book_data(self, book: Book):
        await self._completed_books_loaded.wait()
        book.data = await asyncio.to_thread(get_file_bytes_from_zip, book.link)