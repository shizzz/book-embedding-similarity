from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from chunk_strategy import ChunkStrategy

class PreparedText:
    def __init__(self, strategy: ChunkStrategy, tokens: list[int]):
        self.strategy = strategy
        self.tokens = tokens

    def split(self, max_tokens: int, min_tokens: int, overlap: int, single_chunk_mode: bool):
        return self.strategy.split(self.tokens, max_tokens, min_tokens, overlap, single_chunk_mode)