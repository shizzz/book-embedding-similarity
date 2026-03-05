import os
import asyncio
import numpy as np
import hashlib
import torch
from sentence_transformers import SentenceTransformer
from app.infrastructure.models import Book
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.utils import FB2Book
from app.settings import PathsConfig, ProcessConfig

class Model:
    MODEL_DIR = "models"
    BATCH_SIZE = 16
    EPOCHS = 3
    transformer: SentenceTransformer
    uid: str

    def __init__(self, threads):
        self.name = ProcessConfig.MODEL_NAME
        print("Model:", self.name)
        self._threads = threads
        model_dir = PathsConfig.DATA_DIR / Model.MODEL_DIR
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
        model_dir = PathsConfig.DATA_DIR / Model.MODEL_DIR
        return model_dir / ProcessConfig.MODEL_NAME

    def get_model_uid(self) -> str:
        """Возвращает хеш модели (будет меняться при дообучении)"""
        state_dict = self.transformer.state_dict()
        
        data = b"".join([v.cpu().numpy().tobytes() for v in state_dict.values()])
        return hashlib.md5(data).hexdigest()

    def get_embedding_transformator():    
        return np.load(str(PathsConfig.TRANSFORM_FILE))

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

            batch_size = max(1, int(free_vram_mb * 0.8 / mem_per_chunk_mb))  # margin 10%
            
            # если используем многопоточность, делим batch на количество потоков
            # if hasattr(self, "_threads") and self._threads > 1:
            #     batch_size = max(1, batch_size // self._threads)
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