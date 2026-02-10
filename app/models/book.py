from dataclasses import dataclass
import zipfile
from typing import List, Dict, Any, Optional, Callable, TypeVar
from app.settings.config import BOOK_FOLDER

@dataclass
class Book:
    id: Optional[int]
    archive_name: str
    file_name: str
    title: Optional[str]
    author: Optional[str]
    authors: Optional[List[str]]

    T = TypeVar("T")

    def __init__(
            self,
            archive_name: str,
            file_name: str,
            id: int = None,
            title: str = None,
            author: str = None,
            authors: List[str] = None):
        self.id = id
        self.archive_name = archive_name
        self.file_name = file_name
        self.title = title
        self.author = author

        if authors == None and author != None:
            self.authors = self._parse_authors(author)
        else:
            self.authors = authors

    @classmethod
    def map(cls, row) -> "Book":
        return Book(
            id=row["id"],
            archive_name=row["archive"],
            file_name=row["book"],
            title=row["title"],
            author=row["author"]
        )

    def map_by_id(
        rows: Dict[int, Any],
        mapper: Callable[[Any], T],
    ) -> Dict[int, T]:
        return {
            book_id: mapper(row)
            for book_id, row in rows.items()
        }

    @staticmethod
    def _parse_authors(author: str | None) -> List[str]:
        if not author:
            return []

        return [
            a.strip()
            for a in author.split(",")
            if a.strip()
        ]

    def get_file_bytes_from_zip(self) -> bytes:
        zip_path = f"{BOOK_FOLDER}/{self.archive_name}"

        with zipfile.ZipFile(zip_path, "r") as archive:
            with archive.open(self.file_name) as f:
                return f.read()

@dataclass
class BookRegistry:
    books: list[Book]
    
    def __init__(self, books: list[Book] = None):
        if books:
            self.books = books
        else:
            self.books: List[Book] = []

    def add_books(self, books: list[Book]):
        self.books = books