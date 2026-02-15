import lightgbm as lgb
import numpy as np
import joblib
from app.models import Feedbacks, Embedding, Book
from .rerankerTrainer import RerankerTrainer
from app.settings.config import RERANKER_FILE

class LightGBMRerankerTrainer(RerankerTrainer):
    def train(self, feedbacks: Feedbacks, embeddings, books):
        X, y, w = [], [], []

        for fb in feedbacks.items:
            # пропускаем "нет оценки"
            if fb.label == 0:
                continue

            src = embeddings[fb.source_id]
            tgt = embeddings[fb.candidate_id]

            if src is None or tgt is None:
                continue

            cos = float(
                np.dot(src, tgt)
                / (np.linalg.norm(src) * np.linalg.norm(tgt))
            )

            X.append([cos])

            # используем label напрямую (-1 ... 1)
            y.append(fb.label)

            # отрицательные примеры можно немного усилить
            if fb.label < 0:
                w.append(1.2)
            else:
                w.append(1.0)

        if not X:
            raise ValueError("No training data")

        data = lgb.Dataset(
            np.array(X, dtype=np.float32),
            label=np.array(y, dtype=np.float32),
            weight=np.array(w, dtype=np.float32),
        )

        model = lgb.train(
            {
                "objective": "regression",
                "metric": "rmse",
                "learning_rate": 0.05,
                "num_leaves": 31,
                "min_data_in_leaf": 5,
                "feature_fraction": 1.0,
                "verbosity": -1,
            },
            data,
            num_boost_round=200,
        )

        joblib.dump(model, RERANKER_FILE)