import requests
from typing import Tuple
from app.hnsw import HNSW
from app.model import Model
from app.models import Book, Feedbacks
from app.hnsw.trainers import LightGBMRerankerTrainer
from app.db import db, FeedbackRepository, EmbeddingsRepository, BookRepository
from app.settings.config import LIB_URL, MODEL_NAME

def main():
    with db() as conn:
        sync_feedbacks(conn)
        embeddings = list[Tuple[int, bytes]](EmbeddingsRepository().get_all(conn))
        feedbacks = Feedbacks(FeedbackRepository.get_all(conn))
        books: list[Book] = [
            Book.map_row(row)
            for row in BookRepository().get_all(conn)
        ]
        
    hnsw = HNSW(
        batch_size=10000,
        reranker_trainer=LightGBMRerankerTrainer()
    )
    hnsw.load_emb(embeddings)
    hnsw.rebuild_trainer(feedbacks=feedbacks, books=books)
    print(f"Поисковая модель обновлена")
    print(f"Обучение модели {MODEL_NAME}")

    Model().learn_by_feedback()

def sync_feedbacks(conn):
    url = f"{LIB_URL}/similar/feedback/"

    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as e:
        print(f"Ошибка подключения к feedback API: {e}")
        return

    if resp.status_code != 200:
        print(f"Feedback API вернул статус {resp.status_code}, пропускаем синхронизацию")
        return

    data = resp.json().get("feedback", [])
    feedbacks = Feedbacks.from_dicts(data)
    FeedbackRepository.delete_all(conn)
    feedbacks.insert_feedbacks(conn)

if __name__ == "__main__":
    print(f"Генерация модели поисковой базы на основе фидбеков")
    
    main()