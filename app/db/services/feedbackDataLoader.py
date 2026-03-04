import numpy as np
from app.db import DBRouter
from app.db.repositories import BookRepository
from app.models import Feedbacks, Book
from .bookEmbeddingService import BookEmbeddingService

class FeedbackDataLoader:
    def __init__(self, router: DBRouter):
        self._book_repo = BookRepository(router)
        self._emb_service = BookEmbeddingService(router)

    def load(self, feedbacks: Feedbacks) -> tuple[dict[int, Book], dict[int, np.ndarray]]:
        book_ids = {
            fb.source_id for fb in feedbacks
        } | {
            fb.candidate_id for fb in feedbacks
        }

        embeddings = self._emb_service.get_mean_embeddings(list(book_ids))

        books = self._book_repo.get_many(list(book_ids))

        return books, embeddings