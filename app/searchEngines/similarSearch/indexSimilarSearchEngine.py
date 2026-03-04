import numpy as np
from collections import defaultdict
from faiss import IndexIDMap
from typing import List, Dict
from .similarSearchEngine import SimilarSearchEngine
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.settings.config import IndexLevel

class IndexSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
            self, 
            document_index: IndexIDMap, 
            chunk_index: IndexIDMap, 
            level: IndexLevel, 
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)
        self._document_index = document_index
        self._chunk_index = chunk_index
        self._level = level
        
        self._level_search_map = {
            IndexLevel.DOCUMENT: self._search_document_level,
            IndexLevel.CHUNK: self._search_chunk_level,
            IndexLevel.BOTH: self._search_both_levels
        }

    # ============================================================
    # EMBEDDINGS
    # ============================================================
    def _get_embeddings_by_book_ids(self, book_ids: List[int]):
        data = self._emb_provider.get_by_book_ids(book_ids)
        embeddings, embedding_ids, source_ids = [], [], []

        for emb_id, (vec, book_id) in data.items():
            embeddings.append(vec)
            embedding_ids.append(emb_id)
            source_ids.append(book_id)

        return embeddings, embedding_ids, source_ids

    def _get_embedding_meta(self, embedding_ids: List[int]):
        return self._emb_provider.get_by_ids(list(embedding_ids))


    # ============================================================
    # SEARCH UTILS
    # ============================================================
    def _search_index(self, query_embeddings_np: np.ndarray, index: IndexIDMap, top_k: int):
        distances, ids = index.search(query_embeddings_np, top_k)
        return distances, ids

    # ============================================================
    # LEVEL-SPECIFIC SEARCH
    # ============================================================
    def _search_document_level(self, book_ids: List[int], desired_books: int, top_k_agg: int = None):
        """Поиск только по документам"""
        doc_embs, _, source_ids = self._get_embeddings_by_book_ids(book_ids)
        if not doc_embs:
            return []

        doc_emb_np = np.array(doc_embs)
        k = int(desired_books * self.overfetch_factor)
        distances, ids = self._search_index(doc_emb_np, self._document_index, k)

        result = []
        for q_idx, row in enumerate(ids):
            source_id = source_ids[q_idx]
            for match_idx, candidate_book_id in enumerate(row):
                if candidate_book_id == -1 or candidate_book_id in book_ids:
                    continue
                score = float(distances[q_idx, match_idx])
                result.append({
                    "source_id": source_id,
                    "candidate_id": int(candidate_book_id),
                    "score": score,
                    "matched_chunks": []  # документный поиск без chunk
                })
        return result

    def _search_chunk_level(self, book_ids: List[int], desired_books: int, top_k_agg: int = 5):
        """Поиск только по chunk-level"""
        query_embeddings, query_embedding_ids, source_book_ids = self._get_embeddings_by_book_ids(book_ids)
        if not query_embeddings:
            return []

        query_embeddings_np = np.array(query_embeddings)
        k_chunks = int(desired_books * self.avg_chunks_per_book * self.overfetch_factor)
        distances, embedding_ids_results = self._search_index(query_embeddings_np, self._chunk_index, k_chunks)

        flat_ids = {int(eid) for row in embedding_ids_results for eid in row if eid != -1}
        embedding_meta = self._get_embedding_meta(list(flat_ids))

        # Aggregate matches
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
                score = float(distances[q_idx, match_idx])
                pair_matches[(source_id, candidate_book_id)][(query_embedding_id, candidate_embedding_id)] = score

        # Final aggregation
        candidates = []
        for (source_id, candidate_id), matches in pair_matches.items():
            scores = sorted(matches.values(), reverse=True)[:top_k_agg]
            agg_score = float(np.mean(scores))
            matched_chunks = [
                {
                    "query_embedding_id": qid,
                    "query_embedding": embedding_meta[qid][0],
                    "embedding_id": cid,
                    "embedding": embedding_meta[cid][0],
                    "score": score
                }
                for (qid, cid), score in matches.items() if qid in embedding_meta and cid in embedding_meta
            ]
            candidates.append({
                "source_id": source_id,
                "candidate_id": candidate_id,
                "score": agg_score,
                "matched_chunks": matched_chunks
            })
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates

    def _search_both_levels(self, book_ids: List[int], desired_books: int, top_k_agg: int = 5):
        """Сначала document prefilter, затем chunk-level поиск"""
        # -----------------------------
        # Document prefilter
        # -----------------------------
        doc_result = self._search_document_level(book_ids, int(desired_books * self.avg_chunks_per_book))
        candidate_book_ids = {c["candidate_id"] for c in doc_result}
        if not candidate_book_ids:
            return []

        # -----------------------------
        # Chunk search с фильтром по candidate_book_ids
        # -----------------------------
        query_embeddings, query_embedding_ids, source_book_ids = self._get_embeddings_by_book_ids(book_ids)
        if not query_embeddings:
            return []
        query_embeddings_np = np.array(query_embeddings)
        k_chunks = int(len(candidate_book_ids) * self.avg_chunks_per_book)
        distances, embedding_ids_results = self._search_index(query_embeddings_np, self._chunk_index, k_chunks)

        flat_ids = {int(eid) for row in embedding_ids_results for eid in row if eid != -1}
        embedding_meta = self._get_embedding_meta(list(flat_ids))

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

        # Final aggregation
        candidates = []
        for (source_id, candidate_id), matches in pair_matches.items():
            scores = sorted(matches.values(), reverse=True)[:top_k_agg]
            agg_score = float(np.mean(scores))
            matched_chunks = [
                {
                    "query_embedding_id": qid,
                    "query_embedding": embedding_meta[qid][0],
                    "embedding_id": cid,
                    "embedding": embedding_meta[cid][0],
                    "score": score
                }
                for (qid, cid), score in matches.items() if qid in embedding_meta and cid in embedding_meta
            ]
            candidates.append({
                "source_id": source_id,
                "candidate_id": candidate_id,
                "score": agg_score,
                "matched_chunks": matched_chunks
            })
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates

    # ============================================================
    # MAIN ENTRY
    # ============================================================
    def find_similar_books(self, book_ids: List[int], desired_books: int = 100, top_k_agg: int = 5) -> List[Dict[str, any]]:
        if not book_ids:
            return []
        search_method = self._level_search_map.get(self._level)
        if not search_method:
            raise NotImplementedError(f"Search for level {self._level} is not implemented")
        return search_method(book_ids, desired_books, top_k_agg)