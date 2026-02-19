import asyncio
from typing import AsyncGenerator
from app.db import db, BookRepository
from app.models import Book
from app.utils import FB2Book
from .bookSearchEngine import BaseBookSearchEngine
from sources.remote import RemoteBookScanner

class InpBookSearchEngine(BaseBookSearchEngine):
    TYPE: str = "inpix"

    def __init__(self, folder: str):
        self.folder = folder
        self._completed_books: set[str] = set()
        self._completed_books_loaded = asyncio.Event()

    # -----------------------------
    # Загрузка уже обработанных книг
    # -----------------------------
    def _load_completed_books(self):
        try:
            with db() as conn:
                self._completed_books = set(BookRepository.get_names(conn))
        finally:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(self._completed_books_loaded.set)

    # -----------------------------
    # Парсинг одного файла внутри ZIP
    # -----------------------------
    def _parse(self, zip_bytes, filename) -> list[dict]:
        import zipfile
        books = []
        with zipfile.ZipFile(zip_bytes) as zipf:
            data = zipf.read(filename)
            content = data.decode("utf-8")
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
                    "keywords": fields[13]
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
        if f'{book["file"]}.{book["ext"]}' in self._completed_books:
            return True
        return False

    # -----------------------------
    # Поиск книг (async)
    # -----------------------------
    async def search_books(self) -> AsyncGenerator[Book, None]:
        await asyncio.to_thread(self._load_completed_books)
        await self._completed_books_loaded.wait()

        # Используем RemoteBookScanner как контекстный менеджер
        with RemoteBookScanner(self.folder, self._completed_books) as scanner:
            archives = await asyncio.to_thread(scanner.list_archives)

            for archive in archives:
                archive_bytes = await asyncio.to_thread(scanner._read_archive, archive)
                # Получаем список файлов внутри архива
                books_info = await asyncio.to_thread(self._parse, archive_bytes, archive)

                for book in books_info:
                    if self._should_skip(book):
                        continue

                    authors_list = self._parse_authors(book["author"])
                    authors_str = ", ".join(authors_list)

                    file_name = f'{book["file"]}.{book["ext"]}'
                    link = f"{archive}/{file_name}"

                    yield Book(
                        file_name=file_name,
                        title=book["title"],
                        author=authors_str,
                        authors=authors_list,
                        source_type=self.TYPE,
                        source_link=link
                    )

    # -----------------------------
    # Подсчет общего количества книг
    # -----------------------------
    async def get_total(self) -> int:
        await asyncio.to_thread(self._load_completed_books)
        total = 0

        with RemoteBookScanner(self.folder, self._completed_books) as scanner:
            archives = await asyncio.to_thread(scanner.list_archives)

            for archive in archives:
                archive_bytes = await asyncio.to_thread(scanner._read_archive, archive)
                books_info = await asyncio.to_thread(self._parse, archive_bytes, archive)
                total += sum(1 for book in books_info if not self._should_skip(book))

        return total

    # -----------------------------
    # Получение данных книги
    # -----------------------------
    async def enrich_book_data(self, book: Book):
        archive_name, file_name = book.source_link.split("/", 1)
        with RemoteBookScanner(self.folder, self._completed_books) as scanner:
            book.data = await asyncio.to_thread(scanner.get_book_data, archive_name, file_name)
        fb2 = FB2Book(book.data)
        fb2.enrich_book(book)
