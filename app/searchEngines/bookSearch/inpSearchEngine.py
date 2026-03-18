import asyncio
import os
import zipfile
from datetime import datetime
from typing import AsyncGenerator, List, Dict
from app.workers.stats import Stats
from app.infrastructure.models import Book, BookSearchEngineType
from .bookSearchEngine import BaseBookSearchEngine
from app.settings import PathsConfig

class InpBookSearchEngine(BaseBookSearchEngine):
    def __init__(self, folder: str, stats: Stats = None):
        super().__init__(folder, stats)

        self._archives: List[str] = []
        self._current_archive: str | None = None
        self._loading_tasks: dict[str, asyncio.Task] = {}

    # -----------------------------
    # Парсинг одного файла внутри ZIP
    # -----------------------------
    def _parse(self, zipf: zipfile.ZipFile, filename: str):
        data = zipf.read(filename)

        books = []
        content = data.decode("utf-8", errors="ignore")
        for line in content.splitlines():
            line = line.strip()

            if not line:
                continue

            fields = line.split('\x04')

            while len(fields) <= 13:
                fields.append("")

            books.append({
                "author": fields[0],
                "genere": fields[1],
                "title": fields[2],
                "series": fields[3],
                "serno": fields[4],
                "file": fields[5],
                "size": fields[6],
                "libid": fields[7],
                "deleted": fields[8] == "1",
                "ext": fields[9],
                "date": fields[10],
                "lang": fields[11],
                "librate": fields[12],
                "keywords": fields[13],
            })

        return books

    # -----------------------------
    # Преобразование авторов
    # -----------------------------
    def _parse_authors(self, authors_str: str) -> list[str]:
        authors = []
        for author in authors_str.split(":"):
            author = author.strip()
            if not author:
                continue
            parts = [part.strip() for part in author.split(",") if part.strip()]
            authors.append(" ".join(parts))
        return authors
    
    # -----------------------------
    # Преобразование массивов
    # -----------------------------
    def _parse_array(self, authors_str: str) -> list[str]:
        result = []
        for author in authors_str.split(":"):
            author = author.strip()
            if not author:
                continue
            parts = [part.strip() for part in author.split(",") if part.strip()]
            result.append(" ".join(parts))
        return result

    # -----------------------------
    # Пропуск ненужных книг
    # -----------------------------
    def _should_skip_book(self, book: dict) -> bool:
        if book["lang"] != "ru" or book["deleted"] or not book["file"]:
            return True
        return False

    # -----------------------------
    # Пропуск ненужных архивов
    # -----------------------------
    def _should_skip_archive(self, archive: str) -> bool:
        if archive in ("version.zip", "collection.zip"):
            return True
        return False
    
    # -----------------------------
    # Поиск книг (async)
    # -----------------------------
    async def search_books(self) -> AsyncGenerator[Book, None]:
        zipf = await self._manager.open_zip(PathsConfig.INPX_FOLDER)

        for info in zipf.infolist():
            if info.is_dir():
                continue

            books = await asyncio.to_thread(
                self._parse, 
                zipf, 
                info.filename
            )

            archive_name = os.path.splitext(info.filename)[0] + ".zip"
            self._archives.append(archive_name)

            if self._should_skip_archive(archive_name):
                continue
            
            for book in books:
                if self._should_skip_book(book):
                    continue

                authors = self._parse_authors(book["author"])
                file_name = f"{book['file']}.{book['ext']}"
                link = f"{archive_name}/{file_name}"

                yield Book(
                    file_name=file_name,
                    title=book["title"],
                    author="||".join(authors),
                    authors=authors,
                    serie=book["series"],
                    generes=[g.strip() for g in book["genere"].split(":") if g.strip()],
                    year=datetime.fromisoformat(book["date"]).year,
                    source_type=str(BookSearchEngineType.INPIX),
                    source_link=link
                )

    # -----------------------------
    # Подсчет общего количества книг
    # -----------------------------
    async def get_total(self) -> int:
        zipf = await self._manager.open_zip(PathsConfig.INPX_FOLDER)

        total = 0

        for info in zipf.infolist():
            if info.is_dir():
                continue
            
            archive_name = os.path.splitext(info.filename)[0] + ".zip"     
            if self._should_skip_archive(archive_name):
                continue

            books = await asyncio.to_thread(
                self._parse,
                zipf,
                info.filename
            )

            for book in books:
                if self._should_skip_book(book):
                    continue

                total += 1
        return total
    
    async def _start_prefetch(self, archive: str) -> None:
        task = self._loading_tasks.get(archive)
        if task is not None and not task.done():
            return

        # запускаем фоновую загрузку
        self._loading_tasks[archive] = asyncio.create_task(
            self._manager.load_archive_to_memory(archive)
        )

    def _get_next_archive(self, current_archive: str) -> str | None:
        try:
            idx = self._archives.index(current_archive)
            return self._archives[idx + 1]
        except (ValueError, IndexError):
            return 

    async def get_book_data(self, book: Book) -> bytes:
        archive_name, file_name = book.source_link.split("/", 1)

        # --- 1. если переключились на новый архив ---
        if archive_name != self._current_archive:
            prev_archive = self._current_archive
            self._current_archive = archive_name

            # выгружаем предыдущий архив
            if prev_archive is not None:
                self._manager.unload_archive_from_memory(prev_archive)

            # грузим текущий в память
            await self._start_prefetch(archive_name)

            # запускаем prefetch следующего
            next = self._get_next_archive(archive_name)
            if next:
                await self._start_prefetch()

        # --- 2. читаем файл ---
        return await self._manager.get_book_data(archive_name, file_name)