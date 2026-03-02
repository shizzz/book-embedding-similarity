import numpy as np
from typing import List, Tuple, Dict
from app.hnsw.rerankers import Reranker
from app.db import DBRouter
from app.db.repositories import BookRepository

class SimilarSearchEngine:
    def __init__(
            self,
            limit: int,
            exclude_same_authors: bool,
            router: DBRouter,
            reranker: Reranker = None
        ):
        self._limit = limit
        self._exclude_same_authors = exclude_same_authors
        self._reranker = reranker
        self._router = router
        self.avg_chunks_per_book: int = 7
        self.max_chunks_per_book: int = 50
        self.overfetch_factor: float = 2.5
        self.min_similarity_threshold: float = 0.0

    def find_similar_books(
            self,
            book_ids: List[int],
            desired_books: int = 100,
            top_k_agg: int = 5  # количество чанков для агрегации
        ) -> List[Dict[str, any]]:
        pass

    def enrich_with_db(self, candidates: list[dict], book_repo: BookRepository) -> list[dict]:
        if not candidates:
            return []

        # Список всех уникальных candidate_id
        candidate_ids = list({c['candidate_id'] for c in candidates})

        # Получаем данные из БД
        books_data = book_repo.get_by_ids(candidate_ids)

        # Обогащаем кандидатов
        enriched_candidates = []
        for c in candidates:
            candidate_id = c['candidate_id']
            db_data = books_data.get(candidate_id, {})
            enriched_candidates.append({
                **c,           # source_id, candidate_id, score, matched_chunks
                **db_data      # title, author, serie, generes, year, etc.
            })

        return enriched_candidates
    
    def bulk_filter_candidates(
        self,
        enriched_candidates: list[dict]
    ) -> list[dict]:
        seen_global: set[tuple[int, str, str]] = set()  # (source_id, title, author)
        filtered = []

        for c in enriched_candidates:
            source_id = c['source_id']
            candidate_id = c['candidate_id']

            if source_id == candidate_id:
                continue

            if c.get('book') and c.get('book') == c.get('uid'):
                continue

            if self._exclude_same_authors:
                candidate_author = c.get('author') or ""
                source_author = c.get('source_author') or ""
                if candidate_author and source_author:
                    candidate_authors_set = set(map(str.strip, candidate_author.split(',')))
                    source_authors_set = set(map(str.strip, source_author.split(',')))
                    if candidate_authors_set & source_authors_set:
                        continue

            title = c.get('title') or ""
            author = c.get('author') or ""
            key = (source_id, title, author)
            if key in seen_global:
                continue
            seen_global.add(key)

            filtered.append(c)

        return filtered

    def apply_reranker(
        self,
        filtered_candidates: list[dict]
    ) -> list[dict]:
        if not self._reranker or not self._reranker.model:
            return filtered_candidates

        X = []
        for c in filtered_candidates:
            query_emb = np.mean([m['query_embedding'] for m in c['matched_chunks']], axis=0)
            candidate_emb = np.mean([m['embedding'] for m in c['matched_chunks']], axis=0)

            dot_score = float(np.dot(query_emb, candidate_emb))
            norm_q = np.linalg.norm(query_emb)
            norm_c = np.linalg.norm(candidate_emb)
            cosine_score = dot_score / (norm_q * norm_c + 1e-8)

            source_author = c.get('source_author') or ""
            candidate_author = c.get('author') or ""
            same_author = 1 if source_author and candidate_author and source_author == candidate_author else 0

            source_serie = (c.get('source_serie') or "").strip()
            candidate_serie = (c.get('serie') or "").strip()
            same_serie = 1 if source_serie and candidate_serie and source_serie == candidate_serie else 0

            source_genres = set(map(str.strip, (c.get('source_generes') or "").split(',')))
            candidate_genres = set(map(str.strip, (c.get('generes') or "").split(',')))
            genre_overlap = len(source_genres & candidate_genres)

            year_diff = abs((c.get('source_year') or 0) - (c.get('year') or 0))

            X.append([cosine_score, dot_score, same_author, same_serie, genre_overlap, year_diff])

        X_np = np.array(X, dtype=np.float32)
        try:
            preds = self._reranker.predict(X_np)
            for c, p in zip(filtered_candidates, preds):
                c['score'] = float(p)
        except Exception:
            pass

        return filtered_candidates
    
    def search(
        self,
        sources: List[int],
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        matches_count = int(self._limit * 10 * self.overfetch_factor)
        matches = self.find_similar_books(sources, matches_count)
        enriched = self.enrich_with_db(matches, BookRepository(self._router))
        filtered = self.bulk_filter_candidates(enriched)
        reranked = self.apply_reranker(filtered)

        # возвращаем только топ self._limit в формате Tuple[score, source_id, candidate_id]
        reranked.sort(key=lambda x: x['score'], reverse=True)
        return [(c['score'], c['source_id'], c['candidate_id']) for c in reranked[:self._limit]]

    """
    Контракт find_similar_books

    Возвращает:
        List[Dict], где каждый dict — кандидат для reranker’а:

        {
            "source_id": int,          # ID книги, для которой ищем похожие
            "candidate_id": int,       # ID похожей книги
            "score": float,            # агрегированный score между source и candidate
            "matched_chunks": List[Dict] # список embeddings, с которыми потом считается reranker
                Каждый dict:
                {
                    "query_chunk_id": Optional[int],  # None если виртуальный chunk
                    "query_embedding": np.ndarray,    # embedding источника
                    "chunk_id": Optional[int],        # None если виртуальный chunk
                    "embedding": np.ndarray,          # embedding кандидата
                    "score": float                    # score между query и этим chunk
                }
        }

    Почему так криво:

    1. Память:
       - У нас ~300k книг, каждая с 5–10 chunk’ами → миллионы embeddings.
       - Если использовать dataclass вместо словарей, memory footprint увеличится, объекты занимают больше места.
       - В вебморде с 3GB RAM просто невозможно держать весь index или результат в памяти.

    2. Производительность:
       - Bruteforce перебирает всё, но batching через EmbeddingsBatchIterable + минимальный кэш source embeddings.
       - Если попытаться создавать dataclass на каждый matched_chunk, slowdown на сотни тысяч кандидатов будет заметен.

    3. Контракт:
       - Реранкер ожидает именно словари с `matched_chunks`.
       - Любые попытки упростить структуру или убрать matched_chunks → ломают downstream код.
       - Поэтому приходится держать вложенные словари, даже если это “криво” и тяжеловато читать.

    4. Почему не dataclass:
       - Внутренний Python overhead на сотни тысяч объектов.
       - Долгий GC и большее потребление памяти.
       - Для наших целей словарь — оптимальный компромисс между скоростью, памятью и совместимостью с reranker.

    Итог:
       - Код кривой, но **memory-safe**, поддерживает **контракт reranker** и работает на вебморде с ограниченной памятью.
       - Любые изменения структуры matched_chunks или переход на dataclass → могут привести к OOM или замедлению.
    """