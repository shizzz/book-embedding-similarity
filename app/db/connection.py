import sqlite3
import numpy as np
import zlib
from app.settings.config import DB_FILE

class DB:
    """
    Контекстный менеджер для SQLite с автоматической регистрацией расширений
    """
    def __init__(self, autocommit=True):
        self.autocommit = autocommit
        self.conn = None
        # Регистрируем адаптеры и конвертеры один раз при создании экземпляра
        self._register_adapters()

    def _register_adapters(self):
        # --- Нормализация NumPy ↔ BLOB ---
        def normalize(vec: np.ndarray) -> np.ndarray:
            vec = vec.astype(np.float32)
            norm = np.linalg.norm(vec)
            if norm < 1e-9:
                return np.zeros_like(vec, dtype=np.float32)
            return vec / norm

        sqlite3.register_adapter(np.ndarray, lambda arr: normalize(arr).tobytes())
        sqlite3.register_converter("NUMPY", lambda b: np.frombuffer(b, dtype=np.float32))
        def convert_text(blob: bytes) -> str:
            if blob is None:
                return None
            return zlib.decompress(blob).decode("utf-8")

        sqlite3.register_converter("COMPRESSED_TEXT", convert_text)

    def __enter__(self):
        self.conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                if self.autocommit:
                    self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            self.conn.close()

    def vacuum(self):
        with DB(autocommit=False) as conn:
            conn.execute("VACUUM;")

    @staticmethod
    def adapt_text(text: str) -> bytes:
        if text is None:
            return None
        return zlib.compress(text.encode("utf-8"))