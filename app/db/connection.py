import sqlite3
import numpy as np
import zlib
from contextlib import contextmanager
from app.settings.config import DB_FILE

def normalize(vec: np.ndarray) -> np.ndarray:
    vec = vec.astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm < 1e-9:
        return np.zeros_like(vec, dtype=np.float32)
    return vec / norm

# --- Адаптер для сжатия текста перед записью ---
def adapt_text(text: str) -> bytes:
    if text is None:
        return None
    return zlib.compress(text.encode("utf-8"))

# --- Конвертер для распаковки текста при чтении ---
def convert_text(blob: bytes) -> str:
    if blob is None:
        return None
    return zlib.decompress(blob).decode("utf-8")

# --- Регистрация адаптера NumPy ↔ BLOB ---
sqlite3.register_adapter(np.ndarray, lambda arr: normalize(arr).tobytes())
sqlite3.register_converter("NUMPY", lambda b: np.frombuffer(b, dtype=np.float32))
sqlite3.register_adapter(str, adapt_text)
sqlite3.register_converter("COMPRESSED_TEXT", convert_text)

@contextmanager
def db():
    """
    Контекстный менеджер для подключения к SQLite.
    Поддерживает адаптированные NumPy массивы (float32).
    """
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()