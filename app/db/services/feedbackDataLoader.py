import numpy as np
from app.db import DBRouter
from app.db.repositories import BookRepository
from app.models import Feedbacks, Book
from .bookEmbeddingService import BookEmbeddingService

class PairDataLoader:
    def __init__(self, router: DBRouter):
        self._book_repo = BookRepository(router)
        self._emb_service = BookEmbeddingService(router)

    def _load(self, book_ids: list[int]) -> tuple[dict[int, Book], dict[int, np.ndarray]]:
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