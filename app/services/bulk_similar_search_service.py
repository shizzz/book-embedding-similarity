from typing import List, Tuple
from app.models import Book, Similar, Embedding
from app.searchEngines import BaseSearchEngine

class BulkSimilarSearchService:
    def __init__(
        self,
        engine: BaseSearchEngine,
        books: List[Book],
        embeddings: List[Tuple[int, bytes]],
        logger = None, 
    ):
        self.engine = engine
        self.books = books
        self.embeddings = embeddings
        self.logger = logger

    def run(self, source_book: Book, source_embedding: bytes) -> List[Similar]:
        query_emb = Embedding.from_db(source_embedding)
        return self.engine.search(source_book, query_emb)