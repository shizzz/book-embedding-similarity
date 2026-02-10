import lightgbm as lgb
import numpy as np
import joblib
from app.settings.config import RERANKER_FILE

class LightGBMRerankerTrainer:
    def train(self, feedbacks, embeddings, books):
        X, y, w = [], [], []

        for fb in feedbacks.feedbacks:
            src = embeddings[fb.source_id]
            tgt = embeddings[fb.candidate_id]

            cos = float(
                np.dot(src, tgt)
                / (np.linalg.norm(src) * np.linalg.norm(tgt))
            )

            same_author = int(
                books[fb.source_id].author
                == books[fb.candidate_id].author
            )

            X.append([cos, same_author])
            y.append(1 if fb.label == 1 else 0)
            w.append(1.0)  # сюда можно trust

        data = lgb.Dataset(
            np.array(X),
            label=np.array(y),
            weight=np.array(w),
        )

        model = lgb.train(
            {
                "objective": "binary",
                "metric": "binary_logloss",
                "learning_rate": 0.05,
                "num_leaves": 31,
                "verbose": -1,
            },
            data,
            num_boost_round=200,
        )

        joblib.dump(model, f"{RERANKER_FILE}")
