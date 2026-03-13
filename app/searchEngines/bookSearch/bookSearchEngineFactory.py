from app.workers.stats import Stats
from app.infrastructure.models import BookSearchEngineType
from app.infrastructure.db import DBRouter
from .bookSearchEngine import BaseBookSearchEngine
from .zipBookSearchEngine import ZipBookSearchEngine
from .inpSearchEngine import InpBookSearchEngine
from .dbBookSearchEngine import DbpBookSearchEngine
from app.settings import PathsConfig

ENGINES = {
    BookSearchEngineType.ZIP: ZipBookSearchEngine,
    BookSearchEngineType.INPIX: InpBookSearchEngine,
}

class BookSearchEngineFactory:
    @staticmethod
    def create(
        engine_type: BookSearchEngineType,
        stats: Stats = None,
        router: DBRouter = None,
    ) -> BaseBookSearchEngine:
        if engine_type in ENGINES:
            engine_class = ENGINES[engine_type]
            return engine_class(PathsConfig.BOOK_FOLDER, stats)
        
        if engine_type == BookSearchEngineType.DB:
             return DbpBookSearchEngine(PathsConfig.BOOK_FOLDER, stats, router=router)
        
        raise ValueError(f"Unknown engine_type: {engine_type}")