import numpy as np
from collections import defaultdict
from faiss import IndexIDMap
from typing import List
from .similarSearchEngine import SimilarSearchEngine
from app.settings import SearchIndexLevel
from app.infrastructure.models import ChunkType

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
        data = self._emb_provider.get_by_book_ids(book_ids, ChunkType.TEXT)

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

    def _search_document_level(self, book_ids: List[int], desired_books: int, top_k_agg: int = None):
        s_emb, s_idx = self._get_mean(book_ids)

        k = int(desired_books * self.overfetch_factor)
        distances, ids = self._search_index(s_emb, self._document_index, k)
        result = []

        for q_idx, row in enumerate(ids):
            source_id = s_idx[q_idx]
            for match_idx, candidate_book_id in enumerate(row):
                if candidate_book_id == -1 or candidate_book_id in book_ids: continue
                result.append({"source_id": source_id, "candidate_id": int(candidate_book_id), "score": float(distances[q_idx, match_idx]), "matched_chunks":[]})
        return result

    def _search_chunk_level(self, book_ids: List[int], desired_books: int, top_k_agg: int = 5):
        data = self._emb_provider.get_by_book_ids(book_ids, ChunkType.TEXT)
        if not data: return []

        query_embeddings, query_embedding_ids, source_book_ids = [], [], []
        for emb_id, (vec, book_id, _) in data.items():
            query_embeddings.append(vec)
            query_embedding_ids.append(emb_id)
            source_book_ids.append(book_id)

        query_embeddings_np = np.array(query_embeddings)

        k_chunks = int(desired_books * self.avg_chunks_per_book * self.overfetch_factor)
        distances, embedding_ids_results = self._search_index(query_embeddings_np, self._chunk_index, k_chunks)

        flat_ids = {int(eid) for row in embedding_ids_results for eid in row if eid != -1}
        embedding_meta = self._emb_provider.get_by_ids(list(flat_ids))

        pair_matches, pair_chunks = defaultdict(list), defaultdict(list)

        for q_idx, query_embedding_id in enumerate(query_embedding_ids):
            source_id = source_book_ids[q_idx]

            for match_idx, candidate_embedding_id in enumerate(embedding_ids_results[q_idx]):
                if candidate_embedding_id == -1: continue

                candidate_embedding_id = int(candidate_embedding_id)
                meta = embedding_meta.get(candidate_embedding_id)
                if not meta: continue

                _, candidate_book_id, _ = meta
                score = float(distances[q_idx, match_idx])

                pair_matches[(source_id, candidate_book_id)].append(score)
                pair_chunks[(source_id, candidate_book_id)].append(candidate_embedding_id)

        candidates = []

        for (source_id, candidate_id), scores in pair_matches.items():
            top_scores = sorted(scores, reverse=True)[:top_k_agg]
            agg_score = float(np.mean(top_scores)) * (len(top_scores) / top_k_agg)

            candidates.append({
                "source_id": source_id,
                "candidate_id": candidate_id,
                "score": agg_score,
                "matched_chunks": pair_chunks[(source_id, candidate_id)][:top_k_agg]
            })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates

    def find_similar_books(self, book_ids: List[int], desired_books: int = 100, top_k_agg: int = 5):
        if not book_ids: return []
        search_method = self._level_search_map.get(self._level)
        if not search_method: raise NotImplementedError(f"Search for level {self._level} is not implemented")
        return search_method(book_ids, desired_books, top_k_agg)