import os
import asyncio
import numpy as np
import hashlib
import torch
from typing import Dict, Tuple
from sentence_transformers import SentenceTransformer, InputExample, losses
from torch.utils.data import DataLoader
from app.db import DB, FeedbackRepository, BookRepository, SimilarRepository, EmbeddingsRepository
from app.models import Feedbacks, Book
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.utils import FB2Book
from app.settings.config import MODEL_NAME, DATA_DIR, TRANSFORM_FILE

class Model:
    MODEL_DIR = "models"
    BATCH_SIZE = 16
    EPOCHS = 3
    transformer: SentenceTransformer
    uid: str

    def __init__(self, threads):
        self.name = MODEL_NAME
        print("Model:", self.name)
        self._threads = threads
        model_dir = DATA_DIR / Model.MODEL_DIR
        model_path = Model.get_model_dir()

        model_dir.mkdir(parents=True, exist_ok=True)
        if os.path.exists(model_path):
            self.load_local_model(model_path)
        else:
            transformer = SentenceTransformer(self.name)
            transformer.save(str(model_path))
            del transformer
            self.load_local_model(model_path)
            
        self.uid = self.get_model_uid()
        self._auto_st_params()

        print("CUDA available:", torch.cuda.is_available())
        if torch.cuda.is_available():
            print("CUDA version:", torch.version.cuda)
            print("GPU count:", torch.cuda.device_count())
            print("GPU name:", torch.cuda.get_device_name(0))
        print("Chunk size:", self.st_chunk_size)
        print("Chunk overlap:", self.st_overlap)
        print("Batch size:", self.st_batch_size)

    
    def load_local_model(self, model_path: str):
        self.transformer = SentenceTransformer(
            model_name_or_path=str(model_path),
            #tokenizer_kwargs={"fix_mistral_regex": True}
        )

    @staticmethod
    def get_book_text(book: Book) -> str:
        engine = BookSearchEngineFactory.create(book.source_type)
        asyncio.run(engine.enrich_book_data(book))
        fb2Book = FB2Book(book.data)
        return fb2Book.extract_text()

    @staticmethod
    def get_model_dir():
        model_dir = DATA_DIR / Model.MODEL_DIR
        return model_dir / MODEL_NAME

    def get_model_uid(self) -> str:
        """Возвращает хеш модели (будет меняться при дообучении)"""
        state_dict = self.transformer.state_dict()
        
        data = b"".join([v.cpu().numpy().tobytes() for v in state_dict.values()])
        return hashlib.md5(data).hexdigest()

    def get_embedding_transformator():    
        return np.load(str(TRANSFORM_FILE))

    def learn_by_feedback(self):
        examples = []
        results: Dict[int, Tuple[Book, Book]] = {}

        with DB() as conn:
            feedbacks = Feedbacks(FeedbackRepository.get_all(conn))
            book_ids = set()

            for fb in feedbacks.items:
                book_ids.add(fb.source_id)
                book_ids.add(fb.candidate_id)
            raw_books = BookRepository.get_many(conn, list(book_ids))
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
        train_loss = losses.CosineSimilarityLoss(model=self.transformer)

        # --- Fine-tuning ---
        warmup_steps = max(100, len(train_dataloader) * self.EPOCHS // 10)

        self.transformer.fit(
            train_objectives=[(train_dataloader, train_loss)],
            epochs=self.EPOCHS,
            warmup_steps=warmup_steps,
            show_progress_bar=True
        )

        # --- Сохраняем модель ---
        model_dir = str(Model.get_model_dir())
        self.transformer.save(model_dir)
        print(f"Модель сохранена в {model_dir}")

        self._print_update_model_result(results)
    
    def train_embedding_transform(self):
        X = []  # old embeddings
        Y = []  # new embeddings

        with DB() as conn:
            books = BookRepository.get_all(conn)

            for row in books[:5000]:  # достаточно 1-5k примеров
                book = Book.map_row(row)

                old_emb = EmbeddingsRepository.get(conn, book.id)
                if old_emb is None:
                    continue

                old_norm_emb = old_emb

                text = self.transformer.get_book_text(book)
                new_emb = self.transformer.encode(text)

                X.append(old_norm_emb)
                Y.append(new_emb)

        X = np.stack(X)
        Y = np.stack(Y)

        W, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)

        np.save(str(TRANSFORM_FILE), W)

    def _print_update_model_result(
            self,
            results: Dict[int, Tuple[Book, Book, float]]
    ):
        for candidate_id in results:
            src_book, tgt_book = results[candidate_id]

            src_text = self.get_book_text(src_book)
            tgt_text = self.get_book_text(tgt_book)
            emb_src = self.transformer.encode(src_text)
            emb_tgt = self.transformer.encode(tgt_text)
            score = np.dot(emb_src, emb_tgt)

            with DB() as conn:
                weight = SimilarRepository.get_score(conn, src_book.id, tgt_book.id)

            print(f"Прогнозированная похожесть \"{src_book.title}\" --> \"{tgt_book.title}\": ранее: {weight} теперь: {score}")

    def _auto_st_params(self, overlap_ratio=0.12) -> None:
        # Chunk size и overlap
        chunk_size = self.transformer.max_seq_length * 5  # 1 токен ~ 5 символов
        overlap = int(chunk_size * overlap_ratio)

        # Batch size
        if torch.cuda.is_available():
            free_vram = (
                torch.cuda.get_device_properties(0).total_memory
                - torch.cuda.memory_reserved()
                - torch.cuda.memory_allocated()
            )
            free_vram_mb = free_vram / (1024 ** 2)

            # безопасное эмпирическое потребление на один chunk
            mem_per_chunk_mb = self._estimate_mem_per_chunk_mb(self.transformer.max_seq_length)

            batch_size = max(1, int(free_vram_mb * 0.9 / mem_per_chunk_mb))  # margin 10%
            
            # если используем многопоточность, делим batch на количество потоков
            if hasattr(self, "_threads") and self._threads > 1:
                batch_size = max(1, batch_size // self._threads)
        else:
            batch_size = 8

        self.st_chunk_size = chunk_size
        self.st_overlap = overlap
        self.st_batch_size = batch_size

    def _estimate_mem_per_chunk_mb(self, seq_len_tokens: int) -> float:
        """
        Универсальная оценка памяти на chunk на GPU.
        Работает для любых transformer моделей.
        """
        model = self.transformer._first_module().auto_model
        config = model.config

        hidden = config.hidden_size
        layers = config.num_hidden_layers

        # определяем precision
        dtype = next(model.parameters()).dtype
        bytes_per_param = 2 if dtype in (torch.float16, torch.bfloat16) else 4

        # attention + intermediates coefficient
        K = 1.87

        mem_bytes = seq_len_tokens * hidden * layers * bytes_per_param * K

        return mem_bytes / (1024 ** 2)  
    
    def _measure_mem_per_chunk(self):
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()

        dummy = ["test"] * 32
        self.transformer.encode(dummy, convert_to_numpy=True)
        peak = torch.cuda.max_memory_allocated()

        return peak / 32 / (1024**2)