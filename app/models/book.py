import asyncio
from dataclasses import dataclass
import zipfile
import pickle
import numpy as np
from typing import Optional, List, Tuple
from app.settings.config import BOOK_FOLDER
from app.models.queue import QueueRecord

@dataclass
class BookTask:
    archive_name: str
    file_name: str

    title: Optional[str] = None
    embedding: Optional[str] = None
    authors: Optional[List[str]] = None
    queue: Optional[QueueRecord] = None
    _norm_embedding: Optional[np.ndarray] = None

    completed: bool = False
    in_progress: bool = False
        
    @property
    def norm_embedding(self) -> Optional[np.ndarray]:
        if self._norm_embedding is not None:
            return self._norm_embedding

        if self.embedding is None:
            return None

        try:
            emb = pickle.loads(self.embedding)

            if not isinstance(emb, np.ndarray):
                return None

            norm = np.linalg.norm(emb)
            if norm < 1e-9:
                return None

            self._norm_embedding = (emb / norm).astype(np.float32)
            return self._norm_embedding

        except Exception:
            return None

    def get_file_bytes_from_zip(self) -> bytes:
        zip_path = f"{BOOK_FOLDER}/{self.archive_name}"

        with zipfile.ZipFile(zip_path, "r") as archive:
            with archive.open(self.file_name) as f:
                return f.read()

class BookRegistry:
    def __init__(self):
        self.books: List[BookTask] = []

    def empty(self):
        return len(self.books) == 0

    def bookCount(self) -> int:
        return len(self.books)

    def add_books(self, books: list[BookTask]):
        self.books = books

    def add_books_preload(self, books: list[BookTask]):
        self.books = books
        self.embedding_dim = None

        for book in books:
            if book.embedding is None:
                continue

            try:
                emb = pickle.loads(book.embedding)
                if not isinstance(emb, np.ndarray):
                    continue

                if self.embedding_dim is None:
                    self.embedding_dim = emb.shape[0]
                elif emb.shape[0] != self.embedding_dim:
                    continue

                norm = np.linalg.norm(emb)
                if norm < 1e-9:
                    continue

                norm_emb = (emb / norm).astype(np.float32)
                self.valid_embeddings.append(norm_emb)
                self.valid_books.append(book)

            except Exception:
                continue

    def add_book(
        self,
        archive_name: str,
        file_name: str,
        embedding: Optional[bytes] = None,
        authors: Optional[List[str]] = None,
        completed: bool = False
    ):
        with self._lock:
            self._books.append(
                BookTask(
                    archive_name=archive_name,
                    file_name=file_name,
                    embedding=embedding,
                    authors=authors,
                    completed=completed,
                    in_progress=False
                )
            )

    def bulk_add_from_db(self, rows: list[tuple]):
        with self._lock:
            for archive, book, title, embedding, authors_csv, processed in rows:
                self._books.append(
                    BookTask(
                        archive_name=archive,
                        file_name=book,
                        title=title,
                        embedding=embedding,
                        authors=authors_csv.split(",") if authors_csv else None,
                        completed=processed == 1,
                        in_progress=False
                    )
                )

    def bulk_add_from_queue(self, rows: list[tuple]):
        with self._lock:
            for archive, book, progress, book_count, exclude_same_author in rows:
                self._books.append(
                    BookTask(
                        archive_name = archive,
                        file_name = book,
                        queue = QueueRecord(
                            book = book,
                            progress = progress,
                            book_count = book_count,
                            exclude_same_author = exclude_same_author
                        ),
                        completed = False,
                        in_progress = False
                    )
                )
    
    def get_book_by_name(self, file_name: str, archive_name: str = None) -> BookTask | None:
        for book in self.books:
            if book.file_name == file_name:
                if archive_name is None or book.archive_name == archive_name:
                    return book
        return None
    
    async def get_next_book(self) -> BookTask | None:
        try:
            return await self.queue.get()
        except asyncio.CancelledError:
            return None

    def mark_completed(self):
        self.completed += 1

    def stats(self) -> Tuple[int, int]:
        with self._lock:
            total = len(self._books)
            completed = sum(1 for b in self._books if b.completed)
            return total, completed
        
    def has_pending(self) -> bool:
        with self._lock:
            return any(not b.completed and not b.in_progress for b in self._books)