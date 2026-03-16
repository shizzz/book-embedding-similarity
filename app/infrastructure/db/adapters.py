import sqlite3
import numpy as np
import zlib
from app.settings import ProcessConfig

dtype_model = np.float16 if ProcessConfig.MODEL_EMBEDDING_DTYPE == "float16" else np.float32
dtype = np.float16 if ProcessConfig.STORAGE_EMBEDDING_DTYPE == "float16" else np.float32

class SQLiteAdapters:
    _registered = False

    @classmethod
    def register(cls):
        if cls._registered:
            return

        cls._registered = True

        # ---------- NUMPY ----------
        def normalize(vec: np.ndarray) -> np.ndarray:
            vec = vec.astype(dtype)
            norm = np.linalg.norm(vec)
            if norm < 1e-9:
                return np.zeros_like(vec, dtype=dtype)
            return vec / norm

        def adapt_numpy(arr: np.ndarray) -> bytes:
            return normalize(arr).tobytes()

        def convert_numpy(blob: bytes) -> np.ndarray:
            return np.frombuffer(blob, dtype=dtype)


        sqlite3.register_adapter(np.ndarray, adapt_numpy)
        sqlite3.register_converter("NUMPY", convert_numpy)


        # ---------- COMPRESSED TEXT ----------
        def convert_text(blob: bytes) -> str:
            if blob is None:
                return None

            return zlib.decompress(blob).decode("utf-8")

        sqlite3.register_converter(
            "COMPRESSED_TEXT",
            convert_text
        )

    def adapt_compressed_text(text: str) -> bytes:
        if text is None:
            return None
        return zlib.compress(text.encode("utf-8"))