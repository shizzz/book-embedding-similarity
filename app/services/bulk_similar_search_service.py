from typing import List, Tuple
from app.models import Book, Embedding
from app.searchEngines import SimilarSearchEngine

class BulkSimilarSearchService:
    def __init__(
        self,
        engine: SimilarSearchEngine,
        books: List[Book],
        embeddings: List[Tuple[int, bytes]],
        logger = None, 
    ):
        self.engine = engine
        self.books = books
        self.embeddings = embeddings
        self.logger = logger

    def run(self, source_book: Book, source_embedding: bytes) -> List[Tuple[float, int, int]]:
        query_emb = Embedding.from_db(source_embedding)
        similars = self.engine.search(
            source=source_book, 
            embedding=query_emb
        )

        return similars