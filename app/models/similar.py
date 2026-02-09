from dataclasses import dataclass
from typing import Optional
from app.models.book import Book

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

    def __str__(self):
        return f"{self.score:.3f} — {self.book_id} → {self.similar_book_id}"

    def __repr__(self):
        return f"Similar({self.score:.3f}, {self.book_id!r} → {self.similar_book_id!r})"