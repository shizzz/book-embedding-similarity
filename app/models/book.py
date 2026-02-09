from dataclasses import dataclass
import zipfile
from typing import List
from app.settings.config import BOOK_FOLDER

@dataclass
class Book:
    archive_name: str
    file_name: str
    id: int = None
    title: str = None
    authors: str = None

    def __init__(
            self,
            archive_name: str,
            file_name: str,
            id: int = None,
            title: str = None,
            authors: str = None):
        self.id = id
        self.archive_name = archive_name
        self.file_name = file_name
        self.title = title
        self.authors = authors

    def get_file_bytes_from_zip(self) -> bytes:
        zip_path = f"{BOOK_FOLDER}/{self.archive_name}"

        with zipfile.ZipFile(zip_path, "r") as archive:
            with archive.open(self.file_name) as f:
                return f.read()

class BookRegistry:
    books: list[Book]
    
    def __init__(self, books: list[Book] = None):
        if books:
            self.books = books
        else:
            self.books: List[Book] = []

    def add_books(self, books: list[Book]):
        self.books = books