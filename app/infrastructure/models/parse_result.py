from dataclasses import dataclass
from typing import List
from app.infrastructure.models import Book, Chunk

@dataclass
class ParseResult:
    book: Book
    chunks: List[Chunk]