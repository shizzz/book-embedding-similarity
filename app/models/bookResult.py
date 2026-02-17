from dataclasses import dataclass
from .book import Book

@dataclass
class BookResult:
    source: str
    link: str
    book: Book