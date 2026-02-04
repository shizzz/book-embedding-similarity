from dataclasses import dataclass
from typing import Optional
from app.models.book import Book


@dataclass(frozen=True, slots=True)
class Similar:
    score: float
    source_file_name: str
    candidate_file_name: str
    source: Optional[Book] = None
    candidate: Optional[Book] = None

    @classmethod
    def from_files(
        cls,
        score: float,
        source_file_name: str,
        candidate_file_name: str,
    ) -> "Similar":
        return cls(
            score=score,
            source_file_name=source_file_name,
            candidate_file_name=candidate_file_name,
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
            source_file_name=source.file_name,
            candidate_file_name=candidate.file_name,
            source=source,
            candidate=candidate,
        )

    def fetch_books(self, db) -> None:
        from app.db import DBManager
        db = DBManager()

        if self.source is None:
            self.source = db.get_book(self.source_file_name)
        if self.candidate is None:
            self.candidate = db.get_book(self.candidate_file_name)

    def __str__(self):
        return f"{self.score:.3f} — {self.source_file_name} → {self.candidate_file_name}"

    def __repr__(self):
        return f"Similar({self.score:.3f}, {self.source_file_name!r} → {self.candidate_file_name!r})"