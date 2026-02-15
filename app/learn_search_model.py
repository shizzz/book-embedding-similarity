from typing import Tuple
from app.hnsw import HNSW
from app.models import Book, Feedbacks
from app.hnsw.trainers import LightGBMRerankerTrainer
from app.db import db, FeedbackRepository, EmbeddingsRepository, BookRepository

def main():
    with db() as conn:
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

if __name__ == "__main__":
    print(f"Генерация модели поисковой базы на основе фидбеков")
    
    main()