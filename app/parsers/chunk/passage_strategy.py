from .chunk_strategy import ChunkStrategy
from typing import List
import math

class PassageStrategy(ChunkStrategy):
    prefix = "passage: "

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

        prefix_len = len(self.prefix_tokens)
        # Максимальная длина куска с учётом префикса
        max_chunk_len = max_tokens - prefix_len

        # Если текст помещается в один кусок
        if n + prefix_len <= max_tokens:
            if n < min_tokens and not single_chunk_mode:
                return []
            return [self.prefix_tokens + tokens]

        # Оптимальная длина куска ≈ средняя, чтобы покрыть текст равномерно
        num_chunks = math.ceil(n / max_chunk_len)
        average_chunk_length = min(math.ceil((n + (num_chunks - 1) * overlap) / num_chunks), max_chunk_len)
        ideal_chunk_len = max(average_chunk_length, min_tokens)

        # Шаг = длина куска минус overlap
        step = max(1, ideal_chunk_len)

        chunks = []
        start = 0
        while start < n:
            end = min(start + ideal_chunk_len, n)
            sub_tokens = tokens[start:end]

            if len(sub_tokens) >= min_tokens or single_chunk_mode:
                chunks.append(self.prefix_tokens + sub_tokens)

            if end == n:
                break

            start += step - overlap

        return chunks