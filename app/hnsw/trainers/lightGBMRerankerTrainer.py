import lightgbm as lgb
import numpy as np
import joblib
from .rerankerTrainer import RerankerTrainer
from app.settings.config import RERANKER_FILE

class LightGBMRerankerTrainer(RerankerTrainer):
    def train(self, X, y, groups):
        X_np = np.array(X, dtype=np.float32)
        y_np = np.array(y, dtype=np.int32)

        order = np.argsort(groups)
        X_np = X_np[order]
        y_np = y_np[order]
        groups_sorted = np.array(groups)[order]

        _, group_counts = np.unique(groups_sorted, return_counts=True)

        dataset = lgb.Dataset(
            X_np,
            label=y_np,
            group=group_counts.tolist()
        )

        params = {
            "objective": "lambdarank",
            "metric": "ndcg",
            "learning_rate": 0.07,
            "num_leaves": 31,
            "verbosity": -1,
        }

        model = lgb.train(params, dataset, num_boost_round=300)
        joblib.dump(model, RERANKER_FILE)