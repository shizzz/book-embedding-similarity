from abc import ABC, abstractmethod
from app.infrastructure.models import ParseResult
from .bookParserConfig import ParserConfig

class BookParser(ABC):
    def __init__(
            self,
            filepath: str,
            cnf: ParserConfig
        ):
        self.filepath = filepath
        self.target_chars = cnf.target_chars
        self.min_chars = cnf.min_chars
        self.max_description_chars = cnf.max_description_chars
        self.sections = cnf.sections
        self.prefix_buffer = cnf.prefix_buffer
        self.sections_ratio = cnf.sections_ratio

    @abstractmethod
    def parse(self, data: bytes) -> ParseResult:
        """Парсит книгу и возвращает словарь с метаданными и текстом."""
        pass