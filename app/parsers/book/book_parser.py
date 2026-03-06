from abc import ABC, abstractmethod
from app.infrastructure.models import Book

class BookParser(ABC):
    def __init__(self, filepath: str):
        self.filepath = filepath

    @abstractmethod
    def parse(self, data: bytes) -> Book:
        """Парсит книгу и возвращает словарь с метаданными и текстом."""
        pass