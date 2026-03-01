import lightgbm as lgb
import numpy as np
import joblib
from typing import List, Dict
from app.models import Feedbacks, Book, BookRegistry
from .rerankerTrainer import RerankerTrainer
from app.settings.config import RERANKER_FILE, CHUNK_ID_DIVISOR
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable

class LightGBMRerankerTrainer(RerankerTrainer):
    """
    Trainer для safe learning-to-rank correction на основе пользовательского feedback.
    Модель корректирует FAISS score с учётом признаков книг.
    """
    def get_book_embedding(self, index, book_id: int, max_chunks: int = 50) -> np.ndarray | None:
        """
        Восстанавливает эмбеддинг книги усреднением эмбеддингов чанков из FAISS.
        """
        embeddings = []
        for seq in range(max_chunks):
            chunk_id = book_id * CHUNK_ID_DIVISOR + seq
            try:
                emb = index.reconstruct(chunk_id)
                embeddings.append(emb)
            except Exception:
                break
        if not embeddings:
            return None
        return np.mean(embeddings, axis=0)

    def train(
        self,
        feedbacks: Feedbacks,  # список фидбеков
        index,
        books: BookRegistry,     # словарь book_id -> Book (из БД)
        top_k: int = 100
    ):
        X, y, groups = [], [], []

        for fb in tqdm(feedbacks, desc="Preparing training data"):
            # пропускаем игнор и -1
            if fb.label == 0 or fb.label == -1:
                continue

            src_book = books.get(fb.source_id)
            candidate_book = books.get(fb.candidate_id)
            if not src_book or not candidate_book:
                continue

            # 1. Восстанавливаем эмбеддинги из FAISS
            query_emb = self.get_book_embedding(index, fb.source_id)
            candidate_emb = self.get_book_embedding(index, fb.candidate_id)
            if query_emb is None or candidate_emb is None:
                continue

            # 2. FAISS top-K и ранг
            sims, ids = index.search(query_emb.reshape(1, -1), top_k)
            sims = sims[0]
            ids = ids[0]
            try:
                rank = np.where(ids == fb.candidate_id)[0][0]
                faiss_score = sims[rank]
            except IndexError:
                continue  # candidate не попал в топ-K

            # 3. Вычисляем фичи
            dot_score = float(np.dot(query_emb, candidate_emb))
            cosine_score = dot_score / (np.linalg.norm(query_emb) * np.linalg.norm(candidate_emb) + 1e-8)

            same_author = int(bool(src_book.author and candidate_book.author and src_book.author == candidate_book.author))
            same_serie = int(bool(src_book.serie and candidate_book.serie and src_book.serie == candidate_book.serie))

            src_genres = set(map(str.strip, (src_book.generes or "").split(','))) if src_book.generes else set()
            cand_genres = set(map(str.strip, (candidate_book.generes or "").split(','))) if candidate_book.generes else set()
            genre_overlap = len(src_genres & cand_genres)

            year_diff = abs((src_book.year or 0) - (candidate_book.year or 0))

            # 4. Добавляем фичи, label и group
            X.append([float(faiss_score), float(rank), cosine_score, dot_score, same_author, same_serie, genre_overlap, year_diff])
            y.append(fb.label)
            groups.append(fb.source_id)

        if not X:
            raise ValueError("No training data for LightGBMReranker")

        # 5. Преобразуем в numpy
        X_np = np.array(X, dtype=np.float32)
        y_np = np.array(y, dtype=np.float32)

        # 6. Группировка по source_id для learning-to-rank
        _, group_counts = np.unique(groups, return_counts=True)
        dataset = lgb.Dataset(X_np, label=y_np, group=group_counts.tolist())

        # 7. Параметры LightGBM
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

        # 8. Тренировка
        model = lgb.train(params, dataset, num_boost_round=300)

        # 9. Сохраняем
        joblib.dump(model, RERANKER_FILE)