import sqlite3
from contextlib import contextmanager
import numpy as np
from app.settings.config import DB_FILE

def normalize(vec: np.ndarray) -> np.ndarray:
    vec = vec.astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm < 1e-9:
        return np.zeros_like(vec, dtype=np.float32)
    return vec / norm

# --- Регистрация адаптера NumPy ↔ BLOB ---
sqlite3.register_adapter(np.ndarray, lambda arr: normalize(arr).tobytes())
sqlite3.register_converter("NUMPY", lambda b: np.frombuffer(b, dtype=np.float32))

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