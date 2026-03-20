import numpy as np
from dataclasses import dataclass
from typing import Optional
from typing import Tuple
from app.settings import ProcessConfig
from .constants import ChunkType

dtype = np.float16 if ProcessConfig.STORAGE_EMBEDDING_DTYPE == "float16" else np.float32

@dataclass
class Embedding:
    id: Optional[int] = None
    source_id: Optional[int] = None
    chunk_id: Optional[int] = None
    seq: Optional[int] = None
    data: Optional[np.ndarray] = None
    shape: Optional[int] = None
    type: Optional[ChunkType] = None
    name: Optional[str] = None

    def to_tuple(self) -> Tuple[int, int, int, int, np.ndarray, int, int, str]:
        return (
            self.id, 
            self.source_id, 
            self.chunk_id, 
            self.seq, 
            self.data, 
            self.shape, 
            self.type,
            self.name
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
    
    @staticmethod
    def normalize(vec: np.ndarray) -> np.ndarray:
        vec = vec.astype(np.float32)
        
        if vec.ndim == 1:
            norm = np.linalg.norm(vec)
            if norm < 1e-9:
                return np.zeros_like(vec)
            return vec / norm

        norms = np.linalg.norm(vec, axis=1, keepdims=True)
        norms[norms < 1e-9] = 1.0
        return vec / norms