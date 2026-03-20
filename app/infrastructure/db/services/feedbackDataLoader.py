import numpy as np
from app.infrastructure.models import Feedbacks, Book, ChunkType
from app.infrastructure.providers import EmbeddingProvider, BookProvider
from .bookEmbeddingService import BookEmbeddingService
from app.infrastructure.models import SearchResult

class PairDataLoader:
    def __init__(
            self, 
            book_provider: BookProvider, 
            emb_provider: EmbeddingProvider,
            # Оставляем как опцию, поскольку с тэгами получилось не то чтобы огненно
            include_embeddings: bool = False
        ):
        self._book_provider = book_provider
        self._emb_service = BookEmbeddingService(emb_provider)
        self._include_embeddings = include_embeddings
    
    def _load(
            self, 
            book_ids: list[int],
        ) -> tuple[dict[int, Book], dict[int, dict[ChunkType, np.ndarray]]]:
        embeddings = {}

        if self._include_embeddings:
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

    def load_search(self, candidates: list[SearchResult]) -> tuple[dict[int, Book], dict[int, dict[ChunkType, np.ndarray]]]:
        book_ids = {
            c.Source for c in candidates
        } | {
            c.Candidate for c in candidates
        }
        return self._load(book_ids)