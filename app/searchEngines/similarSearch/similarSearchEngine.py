import logging
import numpy as np
from typing import List, Tuple, Dict, Any
from app.hnsw.rerankers import Reranker
from app.hnsw.services import RerankerFeatureExtractor
from app.infrastructure.db import DBRouter
from app.infrastructure.db.services import PairDataLoader
from app.infrastructure.models import BookPair
from app.infrastructure.embeddings import HybridEmbeddingProvider
from app.infrastructure.books import HybridBookProvider
from app.settings.config import CHUNKS_PER_BOOK, OVERFETCH_FACTOR


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
        self._feature_extractor = RerankerFeatureExtractor()
        # параметры используются при chunk-level агрегации
        self.avg_chunks_per_book: int = CHUNKS_PER_BOOK
        self.overfetch_factor: float = OVERFETCH_FACTOR

        self._book_provider = HybridBookProvider(router)
        self._emb_provider = HybridEmbeddingProvider(router)
        self._data_loader = PairDataLoader(self._book_provider, self._emb_provider)

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

    def bulk_filter_candidates(self, pairs: List[BookPair]) -> List[BookPair]:
        seen_global: set[tuple[int,int]] = set()
        filtered: list[BookPair] = []

        for pair in pairs:
            source_id = pair.source.id
            candidate_id = pair.candidate.id

            # 1. self-match
            if source_id == candidate_id:
                continue

            # 2. фильтр по авторам
            if getattr(self, "_exclude_same_authors", False):
                source_set = {a.strip() for a in (pair.source.author or "").split(",") if a.strip()}
                candidate_set = {a.strip() for a in (pair.candidate.author or "").split(",") if a.strip()}
                if source_set & candidate_set:
                    continue

            # 3. проверка дублей (source_id, candidate_id)
            key = (source_id, candidate_id)
            if key in seen_global:
                continue
            seen_global.add(key)

            # 4. сохраняем в итог
            filtered.append(pair)

        return filtered

    def apply_reranker(self, pairs: List[BookPair]) -> List[BookPair]:
        if not self._reranker or not self._reranker.model:
            return pairs

        extractor = RerankerFeatureExtractor()
        X = []
        valid_pairs = []

        for pair in pairs:
            features = extractor.extract(pair)
            X.append(features)
            valid_pairs.append(pair)

        if not X:
            return pairs

        X_np = np.array(X, dtype=np.float32)

        try:
            preds = self._reranker.predict(X_np)
            for pair, score in zip(valid_pairs, preds):
                pair.score = float(score)
        except Exception as e:
            if self._logger: self._logger.error(e)
            pass

        return pairs

    def search(
        self,
        sources: List[int],
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        # overfetch нужен, чтобы после фильтрации и rerank осталось достаточно кандидатов
        matches = self.find_similar_books(sources, self._limit)
        books, embeddings = self._data_loader.load_search(matches)
        pairs = [
            BookPair.fromSearch(
                source_id=m["source_id"],
                candidate_id=m["candidate_id"],
                score=m["score"],
                books=books,
                embeddings=embeddings,
                meta={
                    "matched_chunks": m["matched_chunks"]
                }
            )
            for m in matches
        ]
        return [
            (pair.score, pair.source.id, pair.candidate.id)
            for pair in sorted(
                (p for p in self.apply_reranker(self.bulk_filter_candidates(pairs)) if p.score > 0),
                key=lambda p: p.score,
                reverse=True
            )[:self._limit]
        ]