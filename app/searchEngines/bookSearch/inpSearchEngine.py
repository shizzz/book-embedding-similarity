import os
import zipfile
import asyncio
from typing import AsyncGenerator
from tqdm.asyncio import tqdm_asyncio
from app.db import db, BookRepository
from app.models import Book
from .bookSearchEngine import BaseBookSearchEngine

class InpBookSearchEngine(BaseBookSearchEngine):
    def __init__(self, folder: str):
        self.folder = folder

    def _load_completed_books(self) -> set[str]:
        with db() as conn:
            self._completed_books = set[str](BookRepository.get_names(conn))

    def _parse(self, zipf, filename):
        data = zipf.read(filename)
        books = []
        
        content = data.decode("utf-8")
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            fields = line.split('\x04')
            
            while len(fields) <= 13:
                fields.append("")

            book = {
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
            }
            books.append(book)
        return books

    def _parse_authors(_, authors_str: str) -> str:
        """
        Преобразует строку авторов 'Фамилия,Имя,Отчество:Фамилия,Имя,Отчество:...'
        в строку 'Фамилия Имя Отчество, Фамилия Имя Отчество'
        """
        authors = []
        for author in authors_str.split(":"):
            author = author.strip()
            if not author:
                continue
            # Разбиваем на части и соединяем через пробел
            parts = [part.strip() for part in author.split(",") if part.strip()]
            authors.append(" ".join(parts))
        return authors

    def _should_skip(self, book) -> bool:
        if book["lang"] != "ru" or book["deleted"] or book["file"] == "":
            return True
            
        if f"{book["file"]}.{book["ext"]}" in self._completed_books:
            return True

        return False

    async def search_books(self) -> AsyncGenerator[Book, None]:
        await asyncio.to_thread(self._load_completed_books)
        
        with zipfile.ZipFile(self.folder) as zipf:
            for info in zipf.infolist():
                if info.is_dir():
                    continue

                books = await asyncio.to_thread(
                    self._parse, zipf, info.filename
                )

                for book in books:
                    authors = self._parse_authors(book["author"])

                    if self._should_skip(book):
                        continue

                    yield Book(
                        archive_name=f"{os.path.splitext(info.filename)[0]}.zip",
                        file_name=f"{book["file"]}.{book["ext"]}",
                        title=book["title"],
                        author=", ".join(authors),
                        authors=authors,
                    )