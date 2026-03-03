import numpy as np
from collections import defaultdict
from faiss import IndexIDMap
from typing import List, Dict
from .similarSearchEngine import SimilarSearchEngine
from app.settings.config import CHUNK_ID_DIVISOR, IndexLevel

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
        self._document_index: IndexIDMap = document_index
        self._chunk_index: IndexIDMap = chunk_index
        self._chunk_id_divisor = CHUNK_ID_DIVISOR
        self._level = level

    def _get_book_id(self, chunk_id: int) -> int:
        """Извлекает book_id из composite chunk_id."""
        return chunk_id // self._chunk_id_divisor

    # === reconstruction helpers ===
    def _reconstruct_chunks_for_books(self, book_ids: List[int], use_virtual_chunks: bool = False):
        embeddings = []
        chunk_ids = []
        sources = []
        reconstructed_cache = {}

        for book_id in book_ids:
            if use_virtual_chunks:
                try:
                    vec = self._document_index.reconstruct(book_id)
                    virtual_chunk_id = book_id * self._chunk_id_divisor
                    embeddings.append(vec)
                    chunk_ids.append(virtual_chunk_id)
                    sources.append(book_id)
                    reconstructed_cache[virtual_chunk_id] = vec
                except Exception:
                    continue
            else:
                for seq in range(self.max_chunks_per_book):
                    chunk_id = book_id * self._chunk_id_divisor + seq
                    try:
                        vec = self._chunk_index.reconstruct(chunk_id)
                        embeddings.append(vec)
                        chunk_ids.append(chunk_id)
                        sources.append(book_id)
                        reconstructed_cache[chunk_id] = vec
                    except Exception:
                        break

        return embeddings, chunk_ids, sources, reconstructed_cache

    # === search helper ===
    def _search_index(self, query_embeddings_np: np.ndarray, index: IndexIDMap, top_k: int):
        distances, ids = index.search(query_embeddings_np, top_k)
        return distances, ids

    # === aggregate matches helper ===
    def _aggregate_chunk_matches(
        self,
        query_chunk_ids: List[int],
        source_ids: List[int],
        candidate_ids: np.ndarray,
        distances: np.ndarray,
        top_k_agg: int,
        candidate_book_ids: set
    ):
        pair_matches = defaultdict(dict)
        for q_idx, query_chunk_id in enumerate(query_chunk_ids):
            source_id = source_ids[q_idx]
            for match_idx, candidate_chunk_id in enumerate(candidate_ids[q_idx]):
                score = distances[q_idx, match_idx]
                if candidate_chunk_id == -1:
                    continue
                candidate_book_id = self._get_book_id(candidate_chunk_id)
                if candidate_book_id not in candidate_book_ids:
                    continue
                pair_key = (source_id, candidate_book_id)
                match_key = (query_chunk_id, candidate_chunk_id)
                pair_matches[pair_key][match_key] = score
        return pair_matches

    # === main find_similar_books ===
    def find_similar_books(
        self,
        book_ids: List[int],
        desired_books: int = 100,
        top_k_agg: int = 5,
    ) -> List[Dict[str, any]]:

        if not book_ids:
            return []

        # --- candidate selection ---
        candidate_book_ids = set()
        if self._level in [IndexLevel.DOCUMENT, IndexLevel.BOTH]:
            doc_embs, _, _, _ = self._reconstruct_chunks_for_books(book_ids, use_virtual_chunks=True)
            if doc_embs:
                doc_emb_np = np.stack(doc_embs)
                k = int(desired_books * self.overfetch_factor)
                distances, ids = self._search_index(doc_emb_np, self._document_index, k)
                for row in ids:
                    for cid in row:
                        if cid != -1 and cid not in book_ids:
                            candidate_book_ids.add(cid)

        if self._level == IndexLevel.CHUNK:
            # fallback: all books are candidates (or chunk search could prefilter)
            candidate_book_ids = set(book_ids)

        if not candidate_book_ids:
            return []

        # --- reconstruct query chunks ---
        use_virtual = self._level in [IndexLevel.DOCUMENT]
        query_embeddings, query_chunk_ids, source_for_chunk, reconstructed_cache = self._reconstruct_chunks_for_books(
            book_ids, use_virtual_chunks=use_virtual
        )

        if not query_embeddings:
            return []

        query_embeddings_np = np.stack(query_embeddings)

        # --- search chunk index ---
        k_chunks = int(top_k_agg * self.avg_chunks_per_book * self.overfetch_factor)
        distances, chunk_ids_results = self._search_index(query_embeddings_np, self._chunk_index, k_chunks)

        # --- aggregate matches ---
        pair_matches = self._aggregate_chunk_matches(
            query_chunk_ids,
            source_for_chunk,
            chunk_ids_results,
            distances,
            top_k_agg,
            candidate_book_ids
        )

        # --- final aggregation ---
        candidates = []
        for (source_id, candidate_id), matches in pair_matches.items():
            scores = sorted(matches.values(), reverse=True)[:top_k_agg]
            agg_score = float(np.mean(scores))
            candidates.append((agg_score, source_id, candidate_id, matches))

        candidates.sort(reverse=True)

        # --- build result (contract preserved) ---
        result = []
        for agg_score, source_id, candidate_id, matches in candidates[:desired_books]:
            matched_chunks = []
            for (query_chunk_id, candidate_chunk_id), score in matches.items():
                query_embedding = reconstructed_cache[query_chunk_id]
                candidate_embedding = self._chunk_index.reconstruct(candidate_chunk_id)
                matched_chunks.append({
                    "query_chunk_id": query_chunk_id,
                    "query_embedding": query_embedding,
                    "chunk_id": candidate_chunk_id,
                    "embedding": candidate_embedding,
                    "score": float(score)
                })

            result.append({
                "source_id": source_id,
                "candidate_id": candidate_id,
                "score": float(agg_score),
                "matched_chunks": matched_chunks
            })

        return result