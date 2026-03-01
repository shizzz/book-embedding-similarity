import numpy as np
from collections import defaultdict
from faiss import IndexIDMap
from typing import List, Dict, Tuple
from app.db import DBRouter
from app.hnsw.rerankers import Reranker
from .similarSearchEngine import SimilarSearchEngine
from app.settings.config import CHUNK_ID_DIVISOR

class IndexSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
        self,
        index,
        limit: int,
        router: DBRouter,
        reranker: Reranker = None,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
        logger = None,
    ):
        super().__init__(
            limit=limit, 
            exclude_same_authors=exclude_same_authors, 
            router=router, 
            reranker=reranker
        )
        self.index: IndexIDMap = index
        self.chunk_id_divisor = CHUNK_ID_DIVISOR
        self.reranker = reranker
        self._step_percent = step_percent
        self.logger = logger

    def _get_book_id(self, chunk_id: int) -> int:
        """Извлекает book_id из composite chunk_id в индексе."""
        return chunk_id // self.chunk_id_divisor
    
    def find_similar_books(
            self,
            book_ids: List[int],
            desired_books: int = 100,
            top_k_agg: int = 5  # количество чанков для агрегации
        ) -> List[Dict[str, any]]:
        if not book_ids:
            return []

        # === 1. Восстановление query embeddings ===
        query_embeddings = []
        query_chunk_ids = []
        source_for_query = []
        reconstructed_cache = {}

        for book_id in book_ids:
            chunk_count = 0
            for seq in range(self.max_chunks_per_book):
                chunk_id = book_id * self.chunk_id_divisor + seq
                try:
                    vec = self.index.reconstruct(chunk_id)
                    query_embeddings.append(vec)
                    query_chunk_ids.append(chunk_id)
                    source_for_query.append(book_id)
                    reconstructed_cache[chunk_id] = vec
                    chunk_count += 1
                except Exception:
                    break
            if chunk_count == 0 and self.logger:
                self.logger.warning(f"Не найдено чанков для source book_id {book_id}.")

        if not query_embeddings:
            if self.logger:
                self.logger.warning("Не удалось восстановить query эмбеддинги для книг.")
            return []

        query_embeddings_np = np.stack(query_embeddings)
        num_query_chunks = len(query_embeddings)

        # === 2. Рассчитываем k для FAISS ===
        k = int(desired_books * self.avg_chunks_per_book * self.overfetch_factor)
        if self.logger:
            self.logger.info(
                f"Ищем top-{k} чанков на query эмбеддинг "
                f"(формула: {desired_books} × {self.avg_chunks_per_book} × {self.overfetch_factor})."
            )

        # === 3. FAISS search ===
        distances, chunk_ids_results = self.index.search(query_embeddings_np, k=k)

        # === 4. Группировка matches по паре (source_id, candidate_id) ===
        pair_matches: Dict[Tuple[int, int], Dict[Tuple[int, int], float]] = defaultdict(dict)
        query_book_ids = set(book_ids)

        for q_idx in range(num_query_chunks):
            source_id = source_for_query[q_idx]
            query_chunk_id = query_chunk_ids[q_idx]
            for match_idx in range(k):
                score = distances[q_idx, match_idx]
                candidate_chunk_id = chunk_ids_results[q_idx, match_idx]
                if candidate_chunk_id == -1 or score < self.min_similarity_threshold:
                    continue
                candidate_id = self._get_book_id(candidate_chunk_id)
                if candidate_id in query_book_ids or candidate_id <= 0 or source_id == candidate_id:
                    continue

                pair_key = (source_id, candidate_id)
                match_key = (query_chunk_id, candidate_chunk_id)
                if match_key not in pair_matches[pair_key] or score > pair_matches[pair_key][match_key]:
                    pair_matches[pair_key][match_key] = score

        # === 5. Агрегация top-K чанков для устойчивого score ===
        candidates = []
        for (source_id, candidate_id), match_scores in pair_matches.items():
            top_scores = sorted(match_scores.values(), reverse=True)[:top_k_agg]
            agg_score = float(np.mean(top_scores))
            matches = [(score, q_chunk, c_chunk) for (q_chunk, c_chunk), score in match_scores.items()]
            candidates.append((agg_score, source_id, candidate_id, matches))

        # Сортируем и берём top desired_books
        candidates.sort(key=lambda x: x[0], reverse=True)
        top_candidates = candidates[:desired_books]

        # === 6. Формируем результат с кэшированными embeddings ===
        result = []
        for agg_score, source_id, candidate_id, matches in top_candidates:
            matched_chunks = []
            for score, query_chunk_id, candidate_chunk_id in matches:
                try:
                    query_embedding = reconstructed_cache[query_chunk_id]
                    candidate_embedding = reconstructed_cache.get(candidate_chunk_id) or self.index.reconstruct(candidate_chunk_id)
                    matched_chunks.append({
                        'query_chunk_id': query_chunk_id,
                        'query_embedding': query_embedding,
                        'chunk_id': candidate_chunk_id,
                        'embedding': candidate_embedding,
                        'score': float(score)
                    })
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Не удалось восстановить эмбеддинги для матча {query_chunk_id}-{candidate_chunk_id}: {e}")
                    continue
            result.append({
                'source_id': source_id,
                'candidate_id': candidate_id,
                'score': float(agg_score),
                'matched_chunks': matched_chunks
            })

        if self.logger:
            self.logger.info(f"Найдено {len(result)} похожих пар книг для reranking.")

        return result