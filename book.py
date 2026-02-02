from dataclasses import dataclass
from threading import Lock
import pickle
import zipfile
import numpy as np
from typing import Optional, List, Tuple
from settings import BOOK_FOLDER

def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    # нормализуем оба вектора
    a_norm = a / np.linalg.norm(a)
    b_norm = b / np.linalg.norm(b)
    return float(np.dot(a_norm, b_norm))

@dataclass
class BookTask:
    archive_name: str
    file_name: str

    title: Optional[str] = None
    embedding: Optional[str] = None
    authors: Optional[List[str]] = None

    completed: bool = False
    in_progress: bool = False

    def get_file_bytes_from_zip(self) -> bytes:
        zip_path = f"{BOOK_FOLDER}/{self.archive_name}"

        with zipfile.ZipFile(zip_path, "r") as archive:
            with archive.open(self.file_name) as f:
                return f.read()


class BookRegistry:
    def __init__(self):
        self._books: List[BookTask] = []
        self._lock = Lock()

    # =====================
    # ADD
    # =====================
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
    
    def get_book_by_name(self, file_name: str, archive_name: str = None) -> BookTask | None:
        with self._lock:
            for book in self._books:
                if book.file_name == file_name:
                    if archive_name is None or book.archive_name == archive_name:
                        return book
        return None
    
    def get_next_book(self) -> Optional[BookTask]:
        with self._lock:
            for book in self._books:
                if not book.completed and not book.in_progress:
                    book.in_progress = True
                    return book
        return None

    def mark_completed(self, book: BookTask):
        with self._lock:
            book.in_progress = False
            book.completed = True

    def stats(self) -> Tuple[int, int]:
        with self._lock:
            total = len(self._books)
            completed = sum(1 for b in self._books if b.completed)
            return total, completed
        
    def has_pending(self) -> bool:
        with self._lock:
            return any(not b.completed and not b.in_progress for b in self._books)

    def find_similar_books(
        self,
        source: BookTask,
        top_k: int = 50,
        exclude_same_authors: bool = True
    ) -> List[Tuple[BookTask, float]]:
        candidates = []

        with self._lock:
            for book in self._books:
                if book.embedding is None:
                    continue

                if source.file_name == book.file_name:
                    continue

                if source.title == book.title:
                    continue

                if exclude_same_authors and source.authors and book.authors:
                    if set(source.authors) & set(book.authors):
                        continue
                    
                query_emb = pickle.loads(source.embedding)
                book_emb  = pickle.loads(book.embedding)

                score = cosine_similarity(query_emb, book_emb)

                candidates.append((book, score))

        # Сортируем по убыванию score и берём top_k
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[:top_k]