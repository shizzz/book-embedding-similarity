from abc import ABC, abstractmethod
from typing import List

# ----------------------------
# Абстрактная стратегия
# ----------------------------
class ChunkStrategy(ABC):
    prefix: str = ""
    prefix_tokens: list[int] = []

    def __init__(self, tokenizer=None):
        if tokenizer and self.prefix:
            self.prefix_tokens = tokenizer.encode(self.prefix, add_special_tokens=False)

    def prepare(self, tokens: list[int]):
        from .prepared_text import PreparedText
        """Возвращает объект PreparedText, готовый к разбиению"""
        return PreparedText(self, tokens)

    @abstractmethod
    def split(
        self,
        tokens: list[int],
        max_chars: int,
        min_chars: int,
        overlap: int,
        single_chunk_mode: bool
    ) -> List[str]:
        pass