from typing import Literal
from .bookSearchEngine import BaseBookSearchEngine
from .zipBookSearchEngine import ZipBookSearchEngine
from .inpSearchEngine import InpBookSearchEngine

class BookSearchEngineFactory:
    ZIP = "zip" 
    INPIX = "inpix"
    
    EngineType = Literal[ZIP, INPIX]

    @staticmethod
    def create(engine_type: str, folder: str) -> BaseBookSearchEngine:
        if engine_type == BookSearchEngineFactory.ZIP:
            return ZipBookSearchEngine(folder)
        if engine_type == BookSearchEngineFactory.INPIX:
            return InpBookSearchEngine(folder)
            
        raise ValueError(f"Unknown engine_type: {engine_type}")