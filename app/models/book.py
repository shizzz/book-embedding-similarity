import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable, TypeVar, Iterator

def safe_get(row, key, default=None):
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default

@dataclass
class Book:
    id: Optional[int]
    file_name: str
    title: Optional[str]
    author: Optional[str]
    authors: Optional[frozenset]
    serie: Optional[str]
    generes: Optional[List[str]]
    year: Optional[int]
    data: Optional[bytes]
    source_type: Optional[str]
    source_link: Optional[str]
    uid: str = None
    embedding: np.ndarray = None
    model_id: int = None
    text: str = None
    shape: int = None
    source_length: int = None


    T = TypeVar("T")

    def __init__(
            self,
            file_name: str,
            id: int = None,
            title: str = None,
            author: str = None,
            authors: List[str] = None,
            serie: str = None,
            generes: List[str] = None,
            year: int = None,
            data: bytes = None,
            source_type: str = None,
            source_link: str = None,
            embedding: np.ndarray = None,
            model_id: int = None,
            text: str = None,
            shape: int = None,
            source_length: int = None
        ):
        self.id = id
        self.file_name = file_name
        self.title = title
        self.author = author
        self.serie = serie
        self.generes = generes
        self.year = year
        self.data = data
        self.source_type = source_type
        self.source_link = source_link
        self.embedding = embedding
        self.model_id = model_id
        self.text = text
        self.shape = shape
        self.source_length = source_length

        self.authors = frozenset(self._parse_array(author)) if authors is None and author else frozenset(authors or [])
        self.authors_key = tuple(sorted(self.authors)) if self.authors else ()
        
    @classmethod
    def map(cls, row) -> "Book":
        if row:
            return Book(
                id=row["id"],
                file_name=row["book"],
                title=row["title"],
                author=row["author"],
                model_id=safe_get(row, "model"),
                serie=safe_get(row, "serie"),
                generes=safe_get(row, "generes").split("||"),
                year=safe_get(row, "year"),
                source_type=safe_get(row, "source_type"),
                source_link=safe_get(row, "source_link"),
                embedding=safe_get(row, "embedding"),
                text=safe_get(row, "source_text"),
                shape=safe_get(row, "shape"),
                source_length=safe_get(row, "source_length"),
            )
        else:
            return None
    
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
    def _parse_array(source: str | None) -> List[str]:
        if not source:
            return []

        return [
            a.strip()
            for a in source.split("||")
            if a.strip()
        ]

    @property
    def source_chunk_length(self):
        return len(self.text)

@dataclass
class BookRegistry:
    books: list[Book]
    
    def __init__(self, books: list[Book] = None):
        self.books = books if books else []
        self._book_map = {book.id: book for book in self.books if book.id is not None}

    # --- container API ---
    def append(self, book: Book) -> None:
        self.books.append(book)
        if book.id is not None:
            self._book_map[book.id] = book

    def clear(self) -> None:
        self.books.clear()

    def copy(self) -> "BookRegistry":
        return BookRegistry(self.books.copy())
    
    # --- standard container methods ---
    def __len__(self) -> int:
        return len(self.books)

    def __iter__(self) -> Iterator[Book]:
        return iter(self.books)

    def __bool__(self) -> bool:
        return bool(self.books)
    
    @property
    def texts(self):
        yield from (book.text for book in self.books)
    
    @property
    def embeddings(self):
        yield from (book.embedding for book in self.books)

    def get(self, book_id: int) -> Optional["Book"]:
        return self._book_map.get(book_id)