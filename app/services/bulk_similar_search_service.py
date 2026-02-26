from typing import List, Tuple
from app.models import BookRegistry
from app.searchEngines.similarSearch import SimilarSearchEngine

class BulkSimilarSearchService:
    def __init__(
        self,
        engine: SimilarSearchEngine,
        logger = None, 
    ):
        self.engine = engine
        self.logger = logger

    def run(self, source_books: BookRegistry) -> List[Tuple[float, int, int]]:
        similars = self.engine.search(sources=source_books)

        return similars