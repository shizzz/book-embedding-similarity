from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass
class Tag:
    id: Optional[int] = None
    parent_id: Optional[int] = None
    name_ru: Optional[str] = None
    name_en: Optional[str] = None

    def to_tuple(self) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        return (
            self.parent_id,
            self.name_ru,
            self.name_en
        )

    @staticmethod
    def from_row(row) -> "Tag":
        return Tag(
            id=row["id"],
            parent_id=row["parent_id"],
            name_ru=row["name_ru"],
            name_en=row.get("name_en")  # на случай, если ключ отсутствует
        )

@dataclass
class BookTag:
    id: Optional[int] = None
    book_id: Optional[int] = None
    genre_id: Optional[int] = None
    model_id: Optional[int] = None

    def to_tuple(self) -> Tuple[Optional[int], int, int, Optional[int]]:
        return (
            self.id,
            self.book_id,
            self.genre_id,
            self.model_id
        )

    @staticmethod
    def from_row(row) -> "BookTag":
        return BookTag(
            id=row["id"],
            book_id=row["book_id"],
            genre_id=row["genre_id"],
            model_id=row["model_id"]
        )