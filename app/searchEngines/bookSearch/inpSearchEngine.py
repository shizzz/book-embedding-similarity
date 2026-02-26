import asyncio
import os
import zipfile
from typing import AsyncGenerator, Any
from app.models import Book
from app.utils import FB2Book
from .bookSearchEngine import BaseBookSearchEngine
from app.searchEngines.sources import RemoteBookScanner
from app.settings.config import INPX_FOLDER

class InpBookSearchEngine(BaseBookSearchEngine):
    TYPE: str = "inpix"

    def __init__(self, folder: str, ui: Any = None):
        self.folder = folder
        self.ui = ui
        self._locks: dict[str, asyncio.Lock] = {}

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
    # Пропуск ненужных книг
    # -----------------------------
    def _should_skip(self, book: dict) -> bool:
        if book["lang"] != "ru" or book["deleted"] or not book["file"]:
            return True
        return False

    # -----------------------------
    # Поиск книг (async)
    # -----------------------------
    async def search_books(self) -> AsyncGenerator[Book, None]:
        with RemoteBookScanner(self.folder, self.ui, self._locks) as scanner:
            zipf = await scanner.open_zip(INPX_FOLDER)
            
            for info in zipf.infolist():
                if info.is_dir():
                    continue

                books = await asyncio.to_thread(
                    self._parse, 
                    zipf, 
                    info.filename
                )

                archive_name = os.path.splitext(info.filename)[0] + ".zip"

                for book in books:
                    if self._should_skip(book):
                        continue

                    authors = self._parse_authors(book["author"])
                    file_name = f"{book['file']}.{book['ext']}"
                    link = f"{archive_name}/{file_name}"

                    yield Book(
                        file_name=file_name,
                        title=book["title"],
                        author=", ".join(authors),
                        authors=authors,
                        source_type=self.TYPE,
                        source_link=link
                    )

    # -----------------------------
    # Подсчет общего количества книг
    # -----------------------------
    async def get_total(self) -> int:
        with RemoteBookScanner(self.folder, self.ui, self._locks) as scanner:
            zipf = await scanner.open_zip(INPX_FOLDER)

            total = 0

            for info in zipf.infolist():
                if info.is_dir():
                    continue

                books = await asyncio.to_thread(
                    self._parse,
                    zipf,
                    info.filename
                )

                for book in books:
                    if self._should_skip(book):
                        continue

                    total += 1
            return total

    # -----------------------------
    # Получение данных книги
    # -----------------------------
    async def enrich_book_data(self, book: Book):
        archive_name, file_name = book.source_link.split("/", 1)
        with RemoteBookScanner(self.folder, self.ui, self._locks) as scanner:
            data = await scanner.get_book_data(archive_name, file_name)
        fb2 = FB2Book(data)
        fb2.enrich_book(book)
