from dataclasses import dataclass
from typing import Optional, List, Tuple
from app.models.book import Book
from app.db import db, BookRepository

@dataclass(frozen=True, slots=True)
class Similar:
    score: float
    book_id: int
    similar_book_id: int
    source: Optional[Book] = None
    candidate: Optional[Book] = None

    @classmethod
    def from_files(
        cls,
        score: float,
        book_id: int,
        similar_book_id: int,
    ) -> "Similar":
        return cls(
            score=score,
            book_id=book_id,
            similar_book_id=similar_book_id,
        )

    @classmethod
    def from_books(
        cls,
        score: float,
        source: Book,
        candidate: Book,
    ) -> "Similar":
        return cls(
            score=score,
            book_id=source.id,
            similar_book_id=candidate.id,
            source=source,
            candidate=candidate,
        )

    @classmethod
    def to_similar_list(
        cls,
        rows: List[Tuple[float, int, int]]
    ) -> List["Similar"]:
        if not rows:
            return []

        book_ids: set[int] = set[int]()
        for _, source_id, candidate_id in rows:
            book_ids.add(source_id)
            book_ids.add(candidate_id)

        with db() as conn:
            raw_books = BookRepository().get_many(conn, list[int](book_ids))
            books_by_id = Book.map_by_id(raw_books, Book.map)

        result: List[Similar] = []
        for score, source_id, candidate_id in rows:
            result.append(
                cls(
                    score=score,
                    book_id=source_id,
                    similar_book_id=candidate_id,
                    source=books_by_id.get(source_id),
                    candidate=books_by_id.get(candidate_id),
                )
            )

        return result

    def __str__(self):
        return f"{self.score:.3f} — {self.book_id} → {self.similar_book_id}"

    def __repr__(self):
        return f"Similar({self.score:.3f}, {self.book_id!r} → {self.similar_book_id!r})"