import os
import numpy as np
import hashlib
import torch
from sentence_transformers import SentenceTransformer
from app.settings import PathsConfig, ProcessConfig

class Model:
    MODEL_DIR = "models"
    TOKEN_TO_CHAR = 5 #: ~token count for one char
    OVERLAP_RATIO = 0.12 #: How much text to connect embeddings
    VRAM_USAGE_RATIO = 1 #: Max memory to fill with chunks
    DEFAULT_BATCH = 8 #: CPU Batch Size 

    transformer: SentenceTransformer

    def __init__(self, threads):
        self.name = ProcessConfig.MODEL_NAME
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

        self._calc_info()
    
    def load_local_model(self, model_path: str):
        self.transformer = SentenceTransformer(
            model_name_or_path=str(model_path),
            #tokenizer_kwargs={"fix_mistral_regex": True}
        )

    @staticmethod
    def get_model_dir():
        model_dir = PathsConfig.DATA_DIR / Model.MODEL_DIR
        return model_dir / ProcessConfig.MODEL_NAME

    def get_embedding_transformator():    
        return np.load(str(PathsConfig.TRANSFORM_FILE))

    def _calc_info(self):
        self.info = ModelInfo()

        max_seq = self.transformer.max_seq_length

        self.info.uid = self._get_model_uid()
        self.info.model_name = self.name
        self.info.estimate_mem_per_chunk_mb = self._estimate_mem_per_chunk_mb(max_seq)
        self.info.st_chunk_size = max_seq * self.TOKEN_TO_CHAR
        self.info.st_overlap = int(self.info.st_chunk_size * self.OVERLAP_RATIO)
        self.info.cuda_available = torch.cuda.is_available()

        batch_size = None

        if self.info.cuda_available:
            cuda_version = torch.version.cuda
            gpu_count = torch.cuda.device_count()
            gpu_name = torch.cuda.get_device_name(0)
            self.info.cuda_version = cuda_version
            self.info.gpu_count = gpu_count
            self.info.gpu_name = gpu_name
            self.info.measure_mem_per_chunk_mb = self._measure_mem_per_chunk()

            mem_per_chunk = self.info.estimate_mem_per_chunk_mb
            batch_size = max(1, int(self.info.free_vram_mb * self.VRAM_USAGE_RATIO / (mem_per_chunk * self._threads)))

        self.info.st_batch_size = batch_size or self.DEFAULT_BATCH

    def _get_model_uid(self) -> str:
        """Возвращает хеш модели (будет меняться при дообучении)"""
        state_dict = self.transformer.state_dict()
        
        data = b"".join([v.cpu().numpy().tobytes() for v in state_dict.values()])
        return hashlib.md5(data).hexdigest()
    
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
    
class ModelInfo:
    model_name: str = None
    uid: str = None
    cuda_available: bool = None
    cuda_version: str = None
    gpu_count: int = None
    gpu_name: str = None
    st_chunk_size: int = None
    st_overlap: int = None
    st_batch_size: int = None
    estimate_mem_per_chunk_mb: int = None
    measure_mem_per_chunk_mb: int = None
    _free_vram_cache: int = None
    _total_vram_mb: int = None

    @property
    def free_vram_mb(self) -> int:
        if self.cuda_available:
            free, total = torch.cuda.mem_get_info()
            return free // 1024 // 1024

    @property
    def total_vram_mb(self) -> int:
        if self._total_vram_mb is None and self.cuda_available:
            self._total_vram_mb = (
                torch.cuda.get_device_properties(0).total_memory // 1024 // 1024
            )
        return self._total_vram_mb