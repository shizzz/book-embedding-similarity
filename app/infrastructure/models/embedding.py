import numpy as np
from dataclasses import dataclass
from typing import Optional
from typing import Tuple
from .constants import ChunkType

@dataclass
class Embedding:
    id: Optional[int] = None
    source_id: Optional[int] = None
    chunk_id: Optional[int] = None
    seq: Optional[int] = None
    data: Optional[np.ndarray] = None
    shape: Optional[int] = None
    type: Optional[ChunkType] = None

    def to_tuple(self) -> Tuple[int, int, int, int, np.ndarray, int, int]:
        return (
            self.id, 
            self.source_id, 
            self.chunk_id, 
            self.seq, 
            self.data, 
            self.shape, 
            self.type
        )

    @staticmethod
    def from_row(row) -> "Embedding":
        return Embedding(
            id=row["id"],
            source_id=row["source_id"],
            chunk_id=row["chunk_id"],
            seq=row["seq"],
            data=row["data"],
            shape=row["shape"],
            type=row["type"]
        )