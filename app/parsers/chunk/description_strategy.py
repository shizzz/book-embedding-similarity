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
        if n <= max_tokens:
            return [self.prefix_tokens + tokens]

        step = max_tokens - overlap
        chunks = []
        for start in range(0, n, step):
            end = min(start + max_tokens, n)
            sub_tokens = tokens[start:end]

            if len(sub_tokens) < min_tokens and not single_chunk_mode:
                continue

            chunks.append(self.prefix_tokens + sub_tokens)

        return chunks