from abc import ABC, abstractmethod
from typing import List, Optional, Callable
from models import Book, Similar

class BaseSearchEngine(ABC):
    def __init__(
        self,
        limit: int,
        step_percent: int = 5,
    ):
        self._limit = limit
        self._step_percent = step_percent

    @abstractmethod
    def search(
        self,
        source_book: Book,
        source_embedding: bytes,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> List[Similar]:
        pass
