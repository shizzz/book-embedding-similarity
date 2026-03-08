from abc import ABC, abstractmethod
from typing import List

# ----------------------------
# Абстрактная стратегия
# ----------------------------
class ChunkStrategy(ABC):
    prefix: str = ""

    def prepare(self, text: str):
        from .prepared_text import PreparedText
        """Возвращает объект PreparedText, готовый к разбиению"""
        return PreparedText(self, self.prefix + text)

    @abstractmethod
    def split(
        self,
        text: str,
        max_chars: int,
        min_chars: int,
        overlap: int,
        single_chunk_mode: bool
    ) -> List[str]:
        pass