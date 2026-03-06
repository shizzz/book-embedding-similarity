from pathlib import Path
from typing import Type, Dict
from .book_parser import BookParser
from .fb2_parser import FB2BookParser

class BookParserFactory:
    _parsers: Dict[str, Type[BookParser]] = {
        ".fb2": FB2BookParser,
    }

    @classmethod
    def create_parser(cls, filepath: str) -> BookParser:
        ext = Path(filepath).suffix.lower()
        parser_cls = cls._parsers.get(ext)
        if not parser_cls:
            raise ValueError(f"No parser available for extension '{ext}'")
        return parser_cls(filepath)