import heapq
import numpy as np
from typing import List
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.db.iterables import EmbeddingsBatchIterable
from .similarSearchEngine import SimilarSearchEngine
from app.infrastructure.models import ChunkType, SearchResult

class BruteforceSimilarSearchEngine(SimilarSearchEngine):

    def __init__(self, batch_size: int = 5000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._batch_size = batch_size

    def find_similar_books(
        self,
        book_ids: List[int],
        desired_books: int = 100,
        top_k_agg: int = 5,
    ) -> List[SearchResult]:

        if not book_ids:
            return []

        repo = EmbeddingsRepository(self._router, self._model_uid)
        source_embeddings = repo.get_by_book_ids(book_ids, ChunkType.TEXT)
        if not source_embeddings:
            return []

        result: List[SearchResult] = []

        for source in source_embeddings:
            heap = []
            source_vec = source.data

            for batch in EmbeddingsBatchIterable(repo, self._batch_size):
                for r in batch:
                    if r.book_id == source.book_id:
                        continue

                    score = float(np.dot(source_vec, r.data))
                    # используем (score, r.id, r) чтобы heapq не сравнивал объекты Embedding
                    heap_item = (score, r.id, r)

                    if len(heap) < desired_books:
                        heapq.heappush(heap, heap_item)
                    else:
                        if score > heap[0][0]:
                            heapq.heapreplace(heap, heap_item)

            # сортируем по score, игнорируем r.id
            top_candidates = sorted(heap, key=lambda x: x[0], reverse=True)

            for score, _, r in top_candidates:
                result.append(SearchResult(
                    Source=source.book_id,
                    Candidate=r.book_id,
                    Score=score,
                    ChunkIds=[source.id]
                ))

        return result