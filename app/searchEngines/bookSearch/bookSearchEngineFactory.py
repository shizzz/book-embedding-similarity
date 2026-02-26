from typing import Literal, Any
from .bookSearchEngine import BaseBookSearchEngine
from .zipBookSearchEngine import ZipBookSearchEngine
from .inpSearchEngine import InpBookSearchEngine
from app.settings.config import BOOK_FOLDER

class BookSearchEngineFactory:
    ZIP = ZipBookSearchEngine.TYPE
    INPIX = InpBookSearchEngine.TYPE
    
    EngineType = Literal[ZIP, INPIX]

    @staticmethod
    def create(engine_type: str, ui: Any = None) -> BaseBookSearchEngine:
        if engine_type == BookSearchEngineFactory.ZIP:
            return ZipBookSearchEngine(BOOK_FOLDER, ui)
        if engine_type == BookSearchEngineFactory.INPIX:
            return InpBookSearchEngine(BOOK_FOLDER, ui)
            
        raise ValueError(f"Unknown engine_type: {engine_type}")