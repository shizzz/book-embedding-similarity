import requests
import tqdm
import argparse
import numpy as np
from typing import Tuple
from app.hnsw import IndexManager
from app.model import Model
from app.models import Book, Feedbacks
from app.hnsw.trainers import LightGBMRerankerTrainer
from app.db import db, FeedbackRepository, EmbeddingsRepository, BookRepository
from app.settings.config import LIB_URL, MODEL_NAME
  
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

def get_data() -> Tuple[list[Tuple[int, bytes]], Feedbacks, list[Book]]:
    with db() as conn:
        sync_feedbacks(conn)
        embeddings = list[Tuple[int, bytes]](EmbeddingsRepository.get_all(conn))
        feedbacks = Feedbacks(FeedbackRepository.get_all(conn))
        books: list[Book] = [
            Book.map_row(row)
            for row in BookRepository.get_all(conn)
        ]

    return (embeddings, feedbacks, books)

def learn_hnsw(embeddings, feedbacks, books):
    print(f"Обновление поисковой модели")
    hnsw = IndexManager(
        batch_size=10000,
        reranker_trainer=LightGBMRerankerTrainer()
    )
    hnsw.load_emb(embeddings)
    hnsw.rebuild_trainer(feedbacks=feedbacks, books=books)
    print(f"Поисковая модель обновлена")

def learn_model():
    print(f"Обучение модели {MODEL_NAME}")
    Model().learn_by_feedback()

def train_trasformator(model):
    model.train_embedding_transform()

def update_embeddings(model: Model, embeddings: list[Tuple[int, bytes]]):
    print(f"Загружаем трансформатор")
    W = model.get_embedding_transformator()

    print(f"Преобразование ембеддингов")
    ids = [book_id for book_id, _ in embeddings]
    emb_array = np.stack([np.frombuffer(b, dtype=np.float32) for _, b in embeddings])
    new_emb_array = emb_array @ W

    with tqdm(
        total=len(ids),
        desc="Сохранение обновленных ембеддингов в базу",
        unit="vec",
        unit_scale=True
    ) as pbar, db() as conn:
            for book_id, new_emb in zip(ids, new_emb_array):
                EmbeddingsRepository.update(conn, book_id, new_emb)
                pbar.update(1)

def add_args(parser: argparse.ArgumentParser):
    parser.add_argument("--learn_hnsw", action="store_true", help="Выполнить learn_hnsw")
    parser.add_argument("--learn_model", action="store_true", help="Выполнить learn_model")
    parser.add_argument("--train_transformer", action="store_true", help="Выполнить train_transformer")
    parser.add_argument("--update_embeddings", action="store_true", help="Выполнить update_embeddings")

def main(args):
    embeddings, feedbacks, books = get_data()

    if args.learn_hnsw:
        print("Запуск learn_hnsw...")
        learn_hnsw(embeddings, feedbacks, books)
    else:
        print("Пропуск learn_hnsw")

    if args.learn_model:
        print("Запуск learn_model...")
        learn_model()
    else:
        print("Пропуск learn_model")

    model = Model()

    if args.train_transformer:
        print("Запуск train_transformer...")
        train_trasformator(model)
    else:
        print("Пропуск train_transformer")

    if args.update_embeddings:
        print("Запуск update_embeddings...")
        update_embeddings(model, embeddings)
    else:
        print("Пропуск update_embeddings")

    print("Процесс завершен")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Генерация модели поисковой базы на основе фидбеков"
    )
    add_args(parser)
    args = parser.parse_args()
    main(args)