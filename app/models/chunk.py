from dataclasses import dataclass, field
from typing import Optional
import zlib

@dataclass
class Chunk:
    book_id: int
    text: str
    chunk_id: Optional[int] = None
    length: int = field(init=False)

    def __post_init__(self):
        self.length = len(self.text)

    # --------- Mapper для meta.db ---------
    def to_tuple_meta(self) -> tuple:
        return (self.chunk_id, self.book_id)

    @staticmethod
    def from_meta_row(row) -> "Chunk":
        return Chunk(
            book_id=row["book_id"],
            text="",
            chunk_id=row["id"]
        )
    
    @staticmethod
    def adapt_compressed_text(text: str) -> bytes:
        if text is None:
            return None
        return zlib.compress(text.encode("utf-8"))

    # --------- Mapper для chunks.db ---------
    def to_tuple_chunks(self) -> tuple:
        return (self.chunk_id, self.book_id, Chunk.adapt_compressed_text(self.text), self.length)

    @staticmethod
    def from_chunks_row(row) -> "Chunk":
        return Chunk(
            book_id=row["book_id"],
            text=row["data"],
            chunk_id=row["id"]
        )