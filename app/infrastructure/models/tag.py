import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple
from .constants import ChunkType

@dataclass
class Tag:
    id: Optional[int] = None
    parent_id: Optional[int] = None
    name_ru: Optional[str] = None
    name_en: Optional[str] = None
    data: Optional[np.ndarray] = None
    type: Optional[ChunkType] = None

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
            name_en=row["name_en"],
            data=row["data"],
            type=row["type"]
        )

@dataclass
class BookTag:
    book_id: int
    genre_id: int
    model_id: int
    distance: float
    id: Optional[int] = None
    type: Optional[ChunkType] = None

    def to_tuple(self) -> Tuple[int, int, int, float]:
        return (
            self.book_id,
            self.genre_id,
            self.model_id,
            self.distance
        )

    @staticmethod
    def from_row(row) -> "BookTag":
        return BookTag(
            id=row["id"],
            book_id=row["book_id"],
            genre_id=row["genre_id"],
            model_id=row["model_id"],
            distance=row["distance"]
        )