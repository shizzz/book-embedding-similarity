import lightgbm as lgb
import numpy as np
import joblib
from .rerankerTrainer import RerankerTrainer
from app.settings.config import RERANKER_FILE

class LightGBMRerankerTrainer(RerankerTrainer):
    """
    Trainer для safe learning-to-rank на основе пользовательского feedback.
    Модель корректирует FAISS score с учётом признаков книг.
    """
    def get_book_embedding(self, book_id: int) -> np.ndarray | None:
        """
        Получаем embedding книги усреднением всех chunk embeddings из БД.
        """
        rows = self._emp_repo.get_embeddings_by_book_ids([book_id])
        if not rows:
            return None
        embeddings = [r.data for r in rows]
        return np.mean(embeddings, axis=0)

    def train(self):
        X, y, groups = [], [], []

        for fb in self._ui.tqdm(self._feedbacks, desc="Preparing training data"):

            src_book = self._book_repo.get_by_id(fb.source_id)
            cand_book = self._book_repo.get_by_id(fb.candidate_id)
            if not src_book or not cand_book:
                continue

            # Восстанавливаем embeddings из БД
            query_emb = self.get_book_embedding(fb.source_id)
            candidate_emb = self.get_book_embedding(fb.candidate_id)
            if query_emb is None or candidate_emb is None:
                continue

            # --- similarity ---
            dot_score = float(np.dot(query_emb, candidate_emb))

            q_norm = np.linalg.norm(query_emb)
            c_norm = np.linalg.norm(candidate_emb)
            cosine_score = dot_score / (q_norm * c_norm + 1e-8)

            # --- мета-фичи ---
            same_author = int(
                src_book.author is not None and
                src_book.author == cand_book.author
            )

            same_serie = int(
                src_book.serie is not None and
                src_book.serie == cand_book.serie
            )

            src_genres = set(src_book.generes)
            cand_genres = set(cand_book.generes)
            genre_overlap = len(src_genres & cand_genres)

            year_diff = abs((src_book.year or 0) - (cand_book.year or 0))

            # --- признаки ---
            X.append([
                cosine_score,
                dot_score,
                same_author,
                same_serie,
                genre_overlap,
                year_diff
            ])

            # --- label преобразование ---
            raw_label = fb.label

            if raw_label <= 0:
                rel = 0
            else:
                # шкала 1–4 (достаточно для малого датасета)
                rel = int(np.clip(round(raw_label * 4), 1, 4))

            y.append(rel)
            groups.append(fb.source_id)

        if not X:
            raise ValueError("No training data for LightGBMReranker")

        X_np = np.array(X, dtype=np.float32)
        y_np = np.array(y, dtype=np.int32)

        # ВАЖНО: lambdarank требует, чтобы данные были сгруппированы подряд
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
            "ndcg_eval_at": [10],
            "learning_rate": 0.05,
            "num_leaves": 63,
            "min_data_in_leaf": 10,
            "feature_fraction": 0.9,
            "verbosity": -1,
        }

        model = lgb.train(params, dataset, num_boost_round=300)

        joblib.dump(model, RERANKER_FILE)
        
    @staticmethod
    def _split_clean(s: str) -> set[str]:
        # убираем пустые строки, чтобы "" не считался жанром/автором
        return {x.strip() for x in s.split(",") if x.strip()}