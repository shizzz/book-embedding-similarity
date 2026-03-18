import os
import numpy as np
import hashlib
import torch
from transformers import AutoTokenizer, AutoModel
from app.settings import PathsConfig, ProcessConfig

class Model:
    MODEL_DIR = "models"
    OVERLAP_RATIO = 0.1 #: How much text to connect embeddings
    VRAM_USAGE_RATIO = 0.95 #: Max memory to fill with chunks
    DEFAULT_BATCH = 8 #: CPU Batch Size

    def __init__(self, threads):
        self.name = ProcessConfig.MODEL_NAME
        self._threads = threads
        model_dir = PathsConfig.DATA_DIR / Model.MODEL_DIR
        model_path = Model.get_model_dir()
        model_dir.mkdir(parents=True, exist_ok=True)

        self.dtype = torch.float16 if ProcessConfig.MODEL_EMBEDDING_DTYPE == "float16" else torch.float32
        self._load(model_path)
        self._calc_info()
    
    def _load(self, model_path: str):
        if os.path.exists(model_path):
            self.tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=True, truncation=False, dtype=self.dtype)
            self.model = AutoModel.from_pretrained(model_path, device_map="auto", dtype=self.dtype)
        else:
            self.tokenizer = AutoTokenizer.from_pretrained(self.name, use_fast=True, truncation=False, dtype=self.dtype)
            self.model = AutoModel.from_pretrained(self.name, device_map="auto", dtype=self.dtype)

            self.tokenizer.save_pretrained(model_path)
            self.model.save_pretrained(model_path)

    @staticmethod
    def get_model_dir():
        model_dir = PathsConfig.DATA_DIR / Model.MODEL_DIR
        return model_dir / ProcessConfig.MODEL_NAME

    def get_embedding_transformator():    
        return np.load(str(PathsConfig.TRANSFORM_FILE))

    def _calc_info(self):
        self.info = ModelInfo()

        self.info.max_seq_length = min(
            self.tokenizer.model_max_length,
            getattr(self.model.config, "max_position_embeddings", 512)
        )
        self.info.st_overlap = int(self.info.max_seq_length * self.OVERLAP_RATIO)
        self.info.uid = self._get_model_uid()
        self.info.model_name = self.name
        self.info.st_chunk_size = self.info.max_seq_length
        self.info.cuda_available = torch.cuda.is_available()
        self.info.vram_usage_ratio = self.VRAM_USAGE_RATIO
        self.info.free_vram_ratio = 1 - self.VRAM_USAGE_RATIO
        self.info.estimate_mem_per_token_mb = self._estimate_mem_per_token_mb()

        batch_size = None

        if self.info.cuda_available:
            cuda_version = torch.version.cuda
            gpu_count = torch.cuda.device_count()
            gpu_name = torch.cuda.get_device_name(0)
            self.info.cuda_version = cuda_version
            self.info.gpu_count = gpu_count
            self.info.gpu_name = gpu_name

            max_tokens_total = self.info.free_vram_mb // self.info.estimate_mem_per_token_mb
            self.info.tokens_per_batch = max(1, int(max_tokens_total // self._threads))

        self.info.st_batch_size = batch_size or self.DEFAULT_BATCH

    def _get_model_uid(self) -> str:
        """Возвращает хеш модели (будет меняться при дообучении)"""
        state_dict = self.model.state_dict()
        
        data = b"".join([v.cpu().numpy().tobytes() for v in state_dict.values()])
        return hashlib.md5(data).hexdigest()
    
    def _estimate_mem_per_token_mb(self, for_training: bool = False) -> float:
        """
        Оценка памяти на один токен на GPU для transformer модели.
        
        for_training: True — учитываются градиенты (для обучения)
        """
        config = self.model.config
        hidden = config.hidden_size
        layers = config.num_hidden_layers

        dtype = next(self.model.parameters()).dtype
        bytes_per_param = 2 if dtype in (torch.float16, torch.bfloat16) else 4

        # 1. Скрытые состояния на всех слоях
        mem_hidden = hidden * layers * bytes_per_param

        # 2. KV кэш для внимания (key + value)
        # shape: 2 * hidden * layers (для одного токена)
        mem_kv = 2 * hidden * layers * bytes_per_param

        # 3. Градиенты (если обучение) — обычно столько же, сколько тензоры
        mem_grad = mem_hidden + mem_kv if for_training else 0

        mem_bytes_per_token = mem_hidden + mem_kv + mem_grad

        return mem_bytes_per_token / (1024 ** 2)  # в МБ
    
class ModelInfo:
    model_name: str = None
    uid: str = None
    max_seq_length: int
    cuda_available: bool = None
    cuda_version: str = None
    gpu_count: int = None
    gpu_name: str = None
    st_chunk_size: int = None
    st_overlap: int = None
    st_batch_size: int = None
    estimate_mem_per_token_mb: int = None
    tokens_per_batch: int = None
    vram_usage_ratio: float = 0
    free_vram_ratio: float = 0

    _free_vram_cache: int = None
    _total_vram_mb: int = None

    @property
    def temp(self) -> int:
        if self.cuda_available:
            return torch.cuda.temperature()
        else:
            return 0

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