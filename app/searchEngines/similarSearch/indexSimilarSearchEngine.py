import numpy as np
from collections import defaultdict
from faiss import IndexIDMap
from typing import List
from app.hnsw import FaissId
from app.infrastructure.models import ChunkType, SearchResult, SearchIndexLevel
from .similarSearchEngine import SimilarSearchEngine

class IndexSimilarSearchEngine(SimilarSearchEngine):
    def __init__(self, document_index: IndexIDMap, chunk_index: IndexIDMap, level: SearchIndexLevel, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._document_index = document_index
        self._chunk_index = chunk_index
        self._level = level
        
        self._level_search_map = {
            SearchIndexLevel.DOCUMENT: self._search_document_level, 
            SearchIndexLevel.CHUNK: self._search_chunk_level
        }

    def _get_mean(self, book_ids: List[int]):
        data = self._emb_provider.get_by_source_ids(book_ids, ChunkType.TEXT)

        book_vectors = {}
        for emb_id, (vec, book_id, type) in data.items():
            book_vectors.setdefault(book_id, []).append(vec)
        doc_embs, source_ids = [], []

        for book_id, vectors in book_vectors.items():
            stacked = np.vstack(vectors)
            merged = np.mean(stacked, axis=0)
            doc_embs.append(merged)
            source_ids.append(book_id)
        return np.array(doc_embs), source_ids

    def _search_index(self, query_embeddings_np: np.ndarray, index: IndexIDMap, top_k: int):
        distances, ids = index.search(query_embeddings_np, top_k)
        return distances, ids

    def _search_document_level(
        self,
        book_ids: List[int],
        desired_books: int,
        top_k_agg: int = None
    ) -> List[SearchResult]:
        s_emb, s_idx = self._get_mean(book_ids)
        k = int(desired_books * self.overfetch_factor)

        distances, ids = self._search_index(
            s_emb,
            self._document_index,
            k
        )

        per_source = defaultdict(list)
        for q_idx, row in enumerate(ids):
            source_id = s_idx[q_idx]

            for match_idx, candidate_book_id in enumerate(row):
                if candidate_book_id == -1 or candidate_book_id == source_id:
                    continue

                per_source[source_id].append(
                    SearchResult(
                        Source=source_id,
                        Candidate=int(candidate_book_id),
                        Score=float(distances[q_idx, match_idx])
                    )
                )

        results = []

        for source_id, items in per_source.items():
            items.sort(key=lambda x: x.Score, reverse=True)
            results.extend(items[:desired_books])

        return results

    def _search_chunk_level(
        self,
        book_ids: List[int],
        desired_books: int,
        top_k_agg: int = 5
    ) -> List[SearchResult]:
        data = self._emb_provider.get_by_source_ids(book_ids, ChunkType.TEXT)
        if not data:
            return []

        query_embeddings = []
        query_books = []

        for _, (vec, book_id, _) in data.items():
            query_embeddings.append(vec)
            query_books.append(book_id)

        query_embeddings_np = np.array(query_embeddings)

        k_chunks = int(
            desired_books *
            self.overfetch_factor
        )

        distances, ids = self._search_index(
            query_embeddings_np,
            self._chunk_index,
            k_chunks
        )

        unpack = FaissId.unpack
        pair_scores = defaultdict(list)
        pair_chunks = defaultdict(list)

        for q_idx, row in enumerate(ids):
            source_book = query_books[q_idx]

            for r_idx, faiss_id in enumerate(row):
                if faiss_id == -1:
                    continue

                candidate_book, chunk_id = unpack(int(faiss_id))
                if candidate_book == source_book:
                    continue

                score = float(distances[q_idx, r_idx])
                key = (source_book, candidate_book)

                pair_scores[key].append(score)
                pair_chunks[key].append(chunk_id)

        results: List[SearchResult] = []

        for (source, candidate), scores in pair_scores.items():
            top_scores = sorted(scores, reverse=True)[:top_k_agg]
            agg_score = float(np.mean(top_scores)) * (len(top_scores) / top_k_agg)

            results.append(
                SearchResult(
                    Source=source,
                    Candidate=candidate,
                    Score=agg_score,
                    ChunkIds=pair_chunks[(source, candidate)][:top_k_agg]
                )
            )

        results.sort(key=lambda x: x.Score, reverse=True)
        return results

    def find_similar_books(self, book_ids: List[int], desired_books: int = 100, top_k_agg: int = 5) -> List[SearchResult]:
        if not book_ids:
            return []

        search_method = self._level_search_map.get(self._level)
        if not search_method:
            raise NotImplementedError(
                f"Search for level {self._level} is not implemented"
            )

        return search_method(book_ids, desired_books, top_k_agg)