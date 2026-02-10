import os
import asyncio
import zipfile
from typing import AsyncGenerator
from tqdm.asyncio import tqdm_asyncio
from app.db import db, BookRepository
from app.models import Book
from .bookSearchEngine import BaseBookSearchEngine

class ZipBookSearchEngine(BaseBookSearchEngine):
    def __init__(self, folder: str):
        self.folder = folder

    def _load_completed_books(self) -> set[str]:
        with db() as conn:
            return set(BookRepository.get_names(conn))

    def _list_archives(self) -> list[str]:
        return [
            f for f in os.listdir(self.folder)
            if f.lower().endswith(".zip")
        ]

    def _scan_archive(self, archive: str, completed_books: set[str]) -> list[Book]:
        archive_path = os.path.join(self.folder, archive)
        result = []

        with zipfile.ZipFile(archive_path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                if info.filename in completed_books:
                    continue

                result.append(
                    Book(
                        archive_name=archive,
                        file_name=info.filename
                    )
                )

        return result
        
    async def search_books(self) -> AsyncGenerator[Book, None]:
        completed_books = await asyncio.to_thread(self._load_completed_books)
        archives = await asyncio.to_thread(self._list_archives)

        for archive in tqdm_asyncio(archives, desc="Проверка архивов", unit=" с", unit_scale=True):
            books = await asyncio.to_thread(
                self._scan_archive,
                archive,
                completed_books
            )

            for book in books:
                yield book