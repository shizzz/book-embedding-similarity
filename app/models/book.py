from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable, TypeVar
from .embedding import Embedding

@dataclass
class Book:
    id: Optional[int]
    file_name: str
    title: Optional[str]
    author: Optional[str]
    authors: Optional[List[str]]
    data: Optional[bytes]
    source_type: Optional[str]
    source_link: Optional[str]
    uid: str = None
    embedding: Embedding = None

    T = TypeVar("T")

    def __init__(
            self,
            file_name: str,
            id: int = None,
            title: str = None,
            author: str = None,
            authors: List[str] = None,
            data: bytes = None,
            source_type: str = None,
            source_link: str = None,
            embedding: Embedding = None):
        self.id = id
        self.file_name = file_name
        self.title = title
        self.author = author
        self.data = data
        self.source_type = source_type
        self.source_link = source_link
        self.embedding = embedding

        if authors == None and author != None:
            self.authors = self._parse_authors(author)
        else:
            self.authors = authors

    @classmethod
    def map(cls, row) -> "Book":
        return Book(
            id=row["id"],
            file_name=row["book"],
            title=row["title"],
            author=row["author"],
            source_type=row["source_type"],
            source_link=row["source_link"],
        )

    @property
    def embedding_bytes(self) -> Optional[bytes]:
        if self.embedding is None:
            return None
        
        return self.embedding.to_db()
    
    @classmethod
    def map_row(cls, row) -> "Book":
        return Book(
            id=row[0],
            file_name=row[1],
            title=row[2],
            author=row[3],
            source_type=row[4],
            source_link=row[5],
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