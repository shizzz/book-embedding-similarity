import sqlite3
import numpy as np
import zlib

class SQLiteAdapters:
    _registered = False

    @classmethod
    def register(cls):
        if cls._registered:
            return

        cls._registered = True

        # ---------- NUMPY ----------
        def normalize(vec: np.ndarray) -> np.ndarray:
            vec = vec.astype(np.float32)
            norm = np.linalg.norm(vec)
            if norm < 1e-9:
                return np.zeros_like(vec, dtype=np.float32)
            return vec / norm

        def adapt_numpy(arr: np.ndarray) -> bytes:
            return normalize(arr).tobytes()

        def convert_numpy(blob: bytes) -> np.ndarray:
            return np.frombuffer(blob, dtype=np.float32)


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