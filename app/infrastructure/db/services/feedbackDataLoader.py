import numpy as np
from app.infrastructure.models import Feedbacks, Book, ChunkType
from app.infrastructure.providers import EmbeddingProvider, BookProvider
from .bookEmbeddingService import BookEmbeddingService

class PairDataLoader:
    def __init__(
            self, 
            book_provider: BookProvider, 
            emb_provider: EmbeddingProvider
        ):
        self._book_provider = book_provider
        self._emb_service = BookEmbeddingService(emb_provider)
    
    def _load(
            self, 
            book_ids: list[int],
        ) -> tuple[dict[int, Book], dict[int, dict[ChunkType, np.ndarray]]]:
        embeddings = self._emb_service.get_mean_embeddings(list[int](book_ids))
        books = self._book_provider.get_many(list[int](book_ids))

        return books, embeddings

    def load(self, feedbacks: Feedbacks) -> tuple[dict[int, Book], dict[int, dict[ChunkType, np.ndarray]]]:
        book_ids = {
            fb.source_id for fb in feedbacks
        } | {
            fb.candidate_id for fb in feedbacks
        }
        return self._load(book_ids)

    def load_search(self, candidates: list[dict]) -> tuple[dict[int, Book], dict[int, dict[ChunkType, np.ndarray]]]:
        book_ids = {
            c["source_id"] for c in candidates
        } | {
            c["candidate_id"] for c in candidates
        }
        return self._load(book_ids)