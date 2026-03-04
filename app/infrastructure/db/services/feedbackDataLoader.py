import numpy as np
from app.infrastructure.models import Feedbacks, Book
from app.infrastructure.db.repositories import BookRepository
from app.infrastructure.providers import EmbeddingProvider
from .bookEmbeddingService import BookEmbeddingService

class PairDataLoader:
    def __init__(
            self, 
            book_repo: BookRepository, 
            emb_provider: EmbeddingProvider
        ):
        self._book_repo = book_repo
        self._emb_service = BookEmbeddingService(emb_provider)
    
    def _load(
            self, 
            book_ids: list[int],
        ) -> tuple[dict[int, Book], dict[int, np.ndarray]]:
        embeddings = self._emb_service.get_mean_embeddings(list(book_ids))
        books = self._book_repo.get_many(list(book_ids))

        return books, embeddings

    def load(self, feedbacks: Feedbacks) -> tuple[dict[int, Book], dict[int, np.ndarray]]:
        book_ids = {
            fb.source_id for fb in feedbacks
        } | {
            fb.candidate_id for fb in feedbacks
        }
        return self._load(book_ids)

    def load_search(self, candidates: list[dict]) -> tuple[dict[int, Book], dict[int, np.ndarray]]:
        book_ids = {
            c["source_id"] for c in candidates
        } | {
            c["candidate_id"] for c in candidates
        }
        return self._load(book_ids)