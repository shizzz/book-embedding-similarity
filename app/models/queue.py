from dataclasses import dataclass
from typing import List

@dataclass
class QueueRecord:
    book: str
    progress: int
    book_count: int
    exclude_same_author: bool

class Queue:
    def __init__(self):
        self._records: List[QueueRecord] = []

    def bulk_add_from_db(self, rows: list[tuple]):
        with self._lock:
            for book, progress, book_count, exclude_same_author in rows:
                self._records.append(
                    QueueRecord(
                        book=book,
                        progress=progress,
                        book_count=book_count,
                        exclude_same_author=exclude_same_author
                    )
                )
    
    def get_book_by_name(self, book: str) -> QueueRecord | None:
        with self._lock:
            for record in self._records:
                if record.book == book:
                    return book
        return None