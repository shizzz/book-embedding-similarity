from abc import ABC, abstractmethod
from typing import List, Tuple
from app.infrastructure.models import Book, Chunk

class BookParser(ABC):
    def __init__(self, filepath: str):
        self.filepath = filepath

    @abstractmethod
    def parse(self, data: bytes) -> Tuple[Book, List[Chunk]]:
        """Парсит книгу и возвращает словарь с метаданными и текстом."""
        pass