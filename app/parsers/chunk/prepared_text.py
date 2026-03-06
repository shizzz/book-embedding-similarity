from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from chunk_strategy import ChunkStrategy

class PreparedText:
    def __init__(self, strategy: ChunkStrategy, text: str):
        self.strategy = strategy
        self.text = text

    def split(self, max_chars: int, min_chars: int, overlap: int, single_chunk_mode: bool):
        return self.strategy.split(self.text, max_chars, min_chars, overlap, single_chunk_mode)