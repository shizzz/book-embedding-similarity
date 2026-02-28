from dataclasses import dataclass
from typing import Optional
from typing import Tuple
import numpy as np

@dataclass
class Embedding:
    data: np.ndarray
    shape: int
    emb_id: Optional[int] = None
    book_id: Optional[int] = None
    chunk_id: Optional[int] = None

    def to_tuple(self) -> Tuple[int, int, int, np.ndarray, int]:
        return (self.emb_id, self.book_id, self.chunk_id, self.data, self.shape)

    @staticmethod
    def from_row(row) -> "Embedding":
        return Embedding(
            emb_id=row["id"],
            book_id=row["book_id"],
            chunk_id=row["chunk_id"],
            data=row["data"],
            shape=row["shape"]
        )