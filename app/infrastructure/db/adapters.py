import sqlite3
import numpy as np
import zlib
from app.settings import ProcessConfig

dtype = np.float16 if ProcessConfig.STORAGE_EMBEDDING_DTYPE == "float16" else np.float32

class SQLiteAdapters:
    _registered = False

    @classmethod
    def register(cls):
        if cls._registered:
            return

        cls._registered = True

        # ---------- NUMPY ----------
        def adapt_numpy(arr: np.ndarray) -> bytes:
            return arr.astype(dtype).tobytes()

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