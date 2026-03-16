from dataclasses import dataclass
from typing import Any
from .constants import ChunkType

@dataclass
class TokenChunk:
    book_id: int
    chunk_id: int
    seq: int
    type: ChunkType
    tokens: Any
    length: int