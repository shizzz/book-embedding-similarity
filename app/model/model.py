import os
import numpy as np
from typing import Dict, Tuple
from sentence_transformers import SentenceTransformer, InputExample, losses
from torch.utils.data import DataLoader
from app.db import db, FeedbackRepository, BookRepository, SimilarRepository
from app.models import Feedbacks, Book
from app.utils import FB2Book
from app.settings.config import MODEL_NAME, DATA_DIR

class Model:
    MODEL_DIR = "models"
    BATCH_SIZE = 16
    EPOCHS = 3
    
    @staticmethod
    def get_book_text(book: Book) -> str:
        data = book.get_file_bytes_from_zip()
        fb2Book = FB2Book(data)
        return fb2Book.extract_text()

    @staticmethod
    def get_model_dir():
        model_dir = DATA_DIR / Model.MODEL_DIR
        return model_dir / MODEL_NAME

    def get(self) -> SentenceTransformer:
        model_dir = DATA_DIR / Model.MODEL_DIR
        model_path = Model.get_model_dir()

        model_dir.mkdir(parents=True, exist_ok=True)
        if os.path.exists(model_path):
            model = SentenceTransformer(str(model_path))
        else:
            model = SentenceTransformer(MODEL_NAME)
            model.save(str(model_path))
        
        return model

    def learn_by_feedback(self):
        examples = []
        results: Dict[int, Tuple[Book, Book]] = {}

        # --- Загружаем модель ---
        model = self.get()

        with db() as conn:
            feedbacks = Feedbacks(FeedbackRepository.get_all(conn))
            book_ids = set()

            for fb in feedbacks.items:
                book_ids.add(fb.source_id)
                book_ids.add(fb.candidate_id)
            raw_books = BookRepository().get_many(conn, list(book_ids))
            books_by_id = Book.map_by_id(raw_books, Book.map)

        for fb in feedbacks.items:
            # Пропускаем нейтральные фидбеки
            if fb.label == 0:
                continue

            src_book = books_by_id.get(fb.source_id)
            tgt_book = books_by_id.get(fb.candidate_id)
            if not src_book or not tgt_book:
                continue

            # Формируем текст: title + author
            src_text = self.get_book_text(src_book)
            tgt_text = self.get_book_text(tgt_book)

            # Нормализация label: -1..1 → 0..1
            score = (fb.label + 1) / 2
            examples.append(InputExample(texts=[src_text, tgt_text], label=score))
            results[fb.id] = (src_book, tgt_book)
            print(f"Обучаем по \"{src_book.title}\" <-- \"{tgt_book.title}\"")

        print(f"Всего обучающих примеров: {len(examples)}")

        # --- DataLoader ---
        train_dataloader = DataLoader(
            examples,
            batch_size=self.BATCH_SIZE,
            shuffle=True,
            pin_memory=False)

        # --- Loss с учётом весов ---
        train_loss = losses.CosineSimilarityLoss(model=model)

        # --- Fine-tuning ---
        warmup_steps = max(100, len(train_dataloader) * self.EPOCHS // 10)

        model.fit(
            train_objectives=[(train_dataloader, train_loss)],
            epochs=self.EPOCHS,
            warmup_steps=warmup_steps,
            show_progress_bar=True
        )

        # --- Сохраняем модель ---
        model_dir = str(Model.get_model_dir())
        model.save(model_dir)
        print(f"Модель сохранена в {model_dir}")

        self._print_update_model_result(results)

    def _print_update_model_result(
            self,
            results: Dict[int, Tuple[Book, Book, float]]
    ):
        model = self.get()
        for candidate_id in results:
            src_book, tgt_book = results[candidate_id]

            src_text = self.get_book_text(src_book)
            tgt_text = self.get_book_text(tgt_book)
            emb_src = model.encode(src_text)
            emb_tgt = model.encode(tgt_text)
            score = np.dot(emb_src, emb_tgt)

            with db() as conn:
                weight = SimilarRepository().get_score(conn, src_book.id, tgt_book.id)

            print(f"Прогнозированная похожесть \"{src_book.title}\" --> \"{tgt_book.title}\": ранее: {weight} теперь: {score}")