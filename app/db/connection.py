# db/connection.py
import sqlite3
from contextlib import contextmanager
from app.settings.config import DB_FILE

@contextmanager
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
