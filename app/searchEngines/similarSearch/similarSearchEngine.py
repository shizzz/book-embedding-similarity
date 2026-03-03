import logging
import numpy as np
from typing import List, Tuple, Dict, Any
from app.hnsw.rerankers import Reranker
from app.db import DBRouter
from app.db.repositories import BookRepository, ModelRepository
from app.settings.config import MODEL_NAME


class SimilarSearchEngine:
    def __init__(
        self,
        limit: int,
        exclude_same_authors: bool,
        router: DBRouter,
        step_percent: int = 1,
        reranker: Reranker = None,
        logger: logging.Logger = None
    ):
        self._limit = limit
        self._exclude_same_authors = exclude_same_authors
        self._reranker = reranker
        self._router = router
        self._logger = logger
        self._step_percent = step_percent
        self._model_uid = ModelRepository(self._router).get_latest_uid(MODEL_NAME)
        # параметры используются при chunk-level агрегации
        self.avg_chunks_per_book: int = 7
        self.overfetch_factor: float = 2.5
        self.min_similarity_threshold: float = 0.0

    def find_similar_books(
        self,
        book_ids: List[int],
        desired_books: int = 100,
        top_k_agg: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Должен вернуть список словарей формата:
        {
            "source_id": int,
            "candidate_id": int,
            "score": float,
            "matched_chunks": [...]
        }
        """
        raise NotImplementedError

    def enrich_with_db(
        self,
        candidates: List[dict],
        book_repo: BookRepository
    ) -> List[dict]:
        if not candidates:
            return []
        # вытаскиваем уникальные candidate_id, чтобы не ходить в БД N раз
        book_ids = list({c["candidate_id"] for c in candidates})
        unique_source_ids = list({c["source_id"] for c in candidates})
        book_ids.extend(unique_source_ids)
        # предполагается, что get_by_ids возвращает dict[id] -> {meta}
        books_data = book_repo.get_by_ids(book_ids)
        enriched = []
        for c in candidates:
            candidate_data = books_data.get(c["candidate_id"], {})
            source_data = books_data.get(c["source_id"], {})
            enriched.append({**c, **candidate_data, "source_data": source_data})
        return enriched

    def bulk_filter_candidates(
        self,
        enriched_candidates: List[dict]
    ) -> List[dict]:
        # защищаемся от дублей (source_id, candidate_id)
        seen_global: set[tuple[int, int]] = set()
        filtered = []
        for c in enriched_candidates:
            source_id = c["source_id"]
            candidate_id = c["candidate_id"]
            # self-match
            if source_id == candidate_id:
                continue
            if self._exclude_same_authors:
                candidate_author = c.get("author") or ""
                source_author = c.get("source_data").get("author") or ""
                if candidate_author and source_author:
                    # сравниваем по пересечению множеств авторов
                    candidate_set = {
                        a.strip() for a in candidate_author.split(",") if a.strip()
                    }
                    source_set = {
                        a.strip() for a in source_author.split(",") if a.strip()
                    }
                    if candidate_set & source_set:
                        continue
            key = (source_id, candidate_id)
            if key in seen_global:
                continue
            seen_global.add(key)
            filtered.append(c)
        return filtered

    @staticmethod
    def _split_clean(s: str) -> set[str]:
        # убираем пустые строки, чтобы "" не считался жанром/автором
        return {x.strip() for x in s.split(",") if x.strip()}

    def apply_reranker(
        self,
        filtered_candidates: List[dict]
    ) -> List[dict]:
        if not self._reranker or not self._reranker.model:
            return filtered_candidates
        X = []
        scored_candidates = []
        for c in filtered_candidates:
            matched = c.get("matched_chunks")
            if not matched:
                continue
            # усредняем эмбеддинги по совпавшим чанкам
            query_emb = np.mean(
                [m["query_embedding"] for m in matched],
                axis=0
            )
            candidate_emb = np.mean(
                [m["embedding"] for m in matched],
                axis=0
            )
            dot_score = float(np.dot(query_emb, candidate_emb))
            norm_q = np.linalg.norm(query_emb)
            norm_c = np.linalg.norm(candidate_emb)
            # косинус через нормализацию dot
            cosine_score = dot_score / (norm_q * norm_c + 1e-8)
            source_data: Dict = c.get("source_data")
            source_author = source_data.get("author") or ""
            candidate_author = c.get("author") or ""
            source_set = self._split_clean(source_author)
            candidate_set = self._split_clean(candidate_author)
            same_author = 1 if source_set & candidate_set else 0
            source_serie = (source_data.get("serie") or "").strip()
            candidate_serie = (c.get("serie") or "").strip()
            same_serie = 1 if (
                source_serie and
                candidate_serie and
                source_serie == candidate_serie
            ) else 0
            source_genres = self._split_clean(source_data.get("generes") or "")
            candidate_genres = self._split_clean(c.get("generes") or "")
            genre_overlap = len(source_genres & candidate_genres)
            year_diff = abs(
                (source_data.get("year") or 0) -
                (c.get("year") or 0)
            )
            X.append([
                cosine_score,
                dot_score,
                same_author,
                same_serie,
                genre_overlap,
                year_diff
            ])
            # сохраняем только тех, для кого построили фичи,
            # чтобы не было рассинхрона при zip(preds)
            scored_candidates.append(c)
        if not X:
            return filtered_candidates
        X_np = np.array(X, dtype=np.float32)
        try:
            preds = self._reranker.predict(X_np)
            for c, p in zip(scored_candidates, preds):
                c["score"] = float(p)
        except Exception:
            # не валим поиск, если reranker упал
            pass
        return filtered_candidates

    def search(
        self,
        sources: List[int],
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        # overfetch нужен, чтобы после фильтрации и rerank осталось достаточно кандидатов
        matches_count = int(self._limit * 10 * self.overfetch_factor)
        matches = self.find_similar_books(sources, matches_count)
        enriched = self.enrich_with_db(
            matches,
            BookRepository(self._router)
        )
        filtered = self.bulk_filter_candidates(enriched)
        reranked = self.apply_reranker(filtered)
        reranked.sort(key=lambda x: x["score"], reverse=True)
        return [
            (c["score"], c["source_id"], c["candidate_id"])
            for c in reranked[:self._limit]
        ]