from typing import Literal
from .bookSearchEngine import BaseBookSearchEngine
from .zipBookSearchEngine import ZipBookSearchEngine
from .inpSearchEngine import InpBookSearchEngine
from app.settings.config import INPX_FOLDER, BOOK_FOLDER

class BookSearchEngineFactory:
    ZIP = ZipBookSearchEngine.TYPE
    INPIX = InpBookSearchEngine.TYPE
    
    EngineType = Literal[ZIP, INPIX]

    @staticmethod
    def create(engine_type: str) -> BaseBookSearchEngine:
        if engine_type == BookSearchEngineFactory.ZIP:
            return ZipBookSearchEngine(BOOK_FOLDER)
        if engine_type == BookSearchEngineFactory.INPIX:
            return InpBookSearchEngine(INPX_FOLDER)
            
        raise ValueError(f"Unknown engine_type: {engine_type}")