import numpy as np
from collections import defaultdict
from faiss import IndexIDMap
from typing import List, Dict
from .similarSearchEngine import SimilarSearchEngine
from app.db.repositories import EmbeddingsRepository
from app.settings.config import IndexLevel

class IndexSimilarSearchEngine(SimilarSearchEngine):

    def __init__(self, document_index: IndexIDMap, chunk_index: IndexIDMap, level: IndexLevel, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._document_index = document_index
        self._chunk_index = chunk_index
        self._level = level
        self._emb_repo = EmbeddingsRepository(self._router, self._model_uid)

    # ============================================================
    # RECONSTRUCTION
    # ============================================================
    def _get_embeddings_by_book_ids(self, book_ids: List[int]):
        """Возвращает embeddings, их ID и source book IDs"""
        rows = self._emb_repo.get_embeddings_by_book_ids(book_ids)
        embeddings, embedding_ids, source_ids = [], [], []
        for r in rows:
            embeddings.append(r.data)
            embedding_ids.append(r.id)
            source_ids.append(r.book_id)
        return embeddings, embedding_ids, source_ids

    # ============================================================
    # SEARCH
    # ============================================================
    def _search_index(self, query_embeddings_np: np.ndarray, index: IndexIDMap, top_k: int):
        distances, ids = index.search(query_embeddings_np, top_k)
        return distances, ids

    # ============================================================
    # MAIN
    # ============================================================
    def find_similar_books(self, book_ids: List[int], desired_books: int = 100, top_k_agg: int = 5) -> List[Dict[str, any]]:
        if not book_ids:
            return []

        # ----------------------------
        # DOCUMENT prefilter
        # ----------------------------
        candidate_book_ids = set()
        if self._level in [IndexLevel.DOCUMENT, IndexLevel.BOTH]:
            doc_embs, _, _ = self._get_embeddings_by_book_ids(book_ids)
            if doc_embs:
                doc_emb_np = np.array(doc_embs)
                k = int(desired_books * self.overfetch_factor)
                distances, ids = self._search_index(doc_emb_np, self._document_index, k)
                for row in ids:
                    for book_id in row:
                        if book_id != -1 and book_id not in book_ids:
                            candidate_book_ids.add(int(book_id))
        elif self._level == IndexLevel.CHUNK:
            candidate_book_ids = set(book_ids)

        if not candidate_book_ids:
            return []

        # ----------------------------
        # Query embeddings (chunk-level)
        # ----------------------------
        query_embeddings, query_embedding_ids, source_book_ids = self._get_embeddings_by_book_ids(book_ids)
        if not query_embeddings:
            return []
        query_embeddings_np = np.array(query_embeddings)

        # ----------------------------
        # Chunk search
        # ----------------------------
        k_chunks = int(desired_books * self.avg_chunks_per_book * self.overfetch_factor * 5)
        distances, embedding_ids_results = self._search_index(query_embeddings_np, self._chunk_index, k_chunks)

        # ----------------------------
        # Resolve embedding_id -> (vector, book_id)
        # ----------------------------
        flat_ids = {int(eid) for row in embedding_ids_results for eid in row if eid != -1}
        embedding_meta = self._emb_repo.get_embeddings_by_ids(list(flat_ids))

        # ----------------------------
        # Aggregate matches
        # ----------------------------
        pair_matches = defaultdict(dict)
        for q_idx, query_embedding_id in enumerate(query_embedding_ids):
            source_id = source_book_ids[q_idx]
            for match_idx, candidate_embedding_id in enumerate(embedding_ids_results[q_idx]):
                if candidate_embedding_id == -1:
                    continue
                candidate_embedding_id = int(candidate_embedding_id)
                if candidate_embedding_id not in embedding_meta:
                    continue
                candidate_vec, candidate_book_id = embedding_meta[candidate_embedding_id]
                if candidate_book_id not in candidate_book_ids:
                    continue
                score = float(distances[q_idx, match_idx])
                pair_matches[(source_id, candidate_book_id)][(query_embedding_id, candidate_embedding_id)] = score

        # ----------------------------
        # Final aggregation
        # ----------------------------
        candidates = []
        for (source_id, candidate_id), matches in pair_matches.items():
            scores = sorted(matches.values(), reverse=True)[:top_k_agg]
            agg_score = float(np.mean(scores))
            candidates.append((agg_score, source_id, candidate_id, matches))
        candidates.sort(reverse=True)

        # ----------------------------
        # Build result
        # ----------------------------
        result = []
        for agg_score, source_id, candidate_id, matches in candidates[:desired_books]:
            matched_chunks = []
            for (query_embedding_id, candidate_embedding_id), score in matches.items():
                query_vec, _ = embedding_meta.get(query_embedding_id, (None, None))
                candidate_vec, _ = embedding_meta.get(candidate_embedding_id, (None, None))
                if query_vec is None:
                    continue
                matched_chunks.append({
                    "query_embedding_id": query_embedding_id,
                    "query_embedding": query_vec,
                    "embedding_id": candidate_embedding_id,
                    "embedding": candidate_vec,
                    "score": score
                })
            result.append({
                "source_id": source_id,
                "candidate_id": candidate_id,
                "score": agg_score,
                "matched_chunks": matched_chunks
            })
        return result