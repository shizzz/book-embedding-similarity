from .chunk_strategy import ChunkStrategy
from typing import List

class DescriptionStrategy(ChunkStrategy):
    prefix = "description: "

    def split(
        self,
        tokens: List[int],
        max_tokens: int,
        min_tokens: int,
        overlap: int,
        single_chunk_mode: bool
    ) -> List[List[int]]:
        n = len(tokens)
        if n == 0:
            return []

        # если текст меньше max_tokens
        if n + len(self.prefix_tokens) <= max_tokens:
            return [self.prefix_tokens + tokens]
        else:
            sub_tokens = tokens[0:max_tokens - len(self.prefix_tokens)]
            return [self.prefix_tokens + sub_tokens]