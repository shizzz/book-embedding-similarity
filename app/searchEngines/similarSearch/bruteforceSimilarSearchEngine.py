import numpy as np
from typing import List, Dict
from collections import defaultdict
from app.db import DBRouter
from app.db.repositories import EmbeddingsRepository
from app.utils import EmbeddingsBatchIterable
from .similarSearchEngine import SimilarSearchEngine

class BruteforceSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
        self,
        db_router: DBRouter,
        model_uid: str,
        limit: int = 100,
        exclude_same_authors: bool = False,
        batch_size: int = 5000,
    ):
        super().__init__(limit=limit, exclude_same_authors=exclude_same_authors, reranker=None)
        self.db_router = db_router
        self.model_uid = model_uid
        self.batch_size = batch_size

    def find_similar_books(
        self,
        book_ids: List[int],
        desired_books: int = 100,
        top_k_agg: int = 5,
    ) -> List[Dict[str, any]]:

        if not book_ids:
            return []

        repo = EmbeddingsRepository(self.db_router, self.model_uid)
        result = []

        # --- Кэш embeddings источников ---
        source_cache = {}

        # --- Получаем embeddings источников заранее ---
        for batch in EmbeddingsBatchIterable(repo, self.batch_size):
            for r in batch:
                if r.book_id in book_ids and r.book_id not in source_cache:
                    source_cache[r.book_id] = r.data
                if len(source_cache) == len(book_ids):
                    break
            if len(source_cache) == len(book_ids):
                break

        if not source_cache:
            return []

        # --- Основной перебор кандидатов ---
        for source_book_id, source_vec in source_cache.items():
            candidates = []
            seen_books = set([source_book_id])

            for batch in EmbeddingsBatchIterable(repo, self.batch_size):
                for r in batch:
                    candidate_id = r.book_id
                    if candidate_id in seen_books:
                        continue
                    seen_books.add(candidate_id)

                    try:
                        score = float(np.dot(source_vec, r.data))
                        matched_chunks = [{
                            "query_chunk_id": None,       # виртуальный
                            "query_embedding": source_vec,
                            "chunk_id": None,             # виртуальный
                            "embedding": r.data,
                            "score": score
                        }]
                        candidates.append({
                            "source_id": source_book_id,
                            "candidate_id": candidate_id,
                            "score": score,
                            "matched_chunks": matched_chunks
                        })
                    except Exception:
                        continue

            # --- Сортируем и берём top-N ---
            candidates.sort(key=lambda x: x["score"], reverse=True)
            result.extend(candidates[:desired_books])

        return result