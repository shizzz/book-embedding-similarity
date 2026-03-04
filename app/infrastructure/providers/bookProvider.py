from typing import Protocol, Dict, Optional, List
from app.infrastructure.models import Book


class BookProvider(Protocol):
    def get_by_id(self, book_id: int) -> Optional[Book]:
        ...

    def get_many(self, book_ids: List[int]) -> Dict[int, Book]:
        ...