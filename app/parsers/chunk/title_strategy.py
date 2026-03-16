from .chunk_strategy import ChunkStrategy
from typing import List

class TitleStrategy(ChunkStrategy):
    prefix = "title: "

    def split(
        self,
        tokens: List[int],
        max_tokens: int,
        min_tokens: int,
        overlap: int,
        single_chunk_mode: bool
    ) -> List[List[int]]:
        # просто возвращаем один chunk с префиксом
        return [self.prefix_tokens + tokens]