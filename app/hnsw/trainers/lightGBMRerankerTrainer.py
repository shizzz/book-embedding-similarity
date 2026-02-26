import lightgbm as lgb
import numpy as np
import joblib
from typing import List
from app.models import Feedbacks, Book
from .rerankerTrainer import RerankerTrainer
from app.settings.config import RERANKER_FILE
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable

class LightGBMRerankerTrainer(RerankerTrainer):
    """
    Trainer для safe learning-to-rank correction на основе пользовательского feedback.
    Модель учится корректировать FAISS score, не заменяя его полностью.
    """

    def train(
            self, 
            feedbacks: Feedbacks, 
            index, 
            books: List[Book], 
            top_k: int = 100
        ):
        X, y, groups = [], [], []

        for fb in tqdm(feedbacks.items, desc="Preparing training data"):
            if fb.label == 0:
                continue

            src_book = books[fb.source_id]

            # FAISS top-K для source
            sims, ids = index.search(src_book.embedding.reshape(1, -1), top_k)
            sims = sims[0]
            ids = ids[0]

            # находим позицию candidate в FAISS ranking
            try:
                rank = np.where(ids == fb.candidate_id)[0][0]
                faiss_score = sims[rank]
            except IndexError:
                continue  # candidate не попал в top-K, пропускаем

            # Фичи: score и rank
            X.append([float(faiss_score), float(rank)])
            y.append(fb.label)

            # group для lambdarank
            groups.append(fb.source_id)

        if not X:
            raise ValueError("No training data")

        X = np.array(X, dtype=np.float32)
        y = np.array(y, dtype=np.int32)

        # Группировка по source_id для ranking objective
        _, group_counts = np.unique(groups, return_counts=True)

        dataset = lgb.Dataset(
            X,
            label=y,
            group=group_counts.tolist()
        )

        params = {
            "objective": "lambdarank",
            "metric": "ndcg",
            "ndcg_eval_at": [10],
            "learning_rate": 0.05,
            "num_leaves": 63,
            "min_data_in_leaf": 10,
            "feature_fraction": 0.9,
            "verbosity": -1,
        }

        model = lgb.train(
            params,
            dataset,
            num_boost_round=300
        )

        # Сохраняем модель
        joblib.dump(model, RERANKER_FILE)