import sqlite3
import threading
from pathlib import Path
from .adapters import SQLiteAdapters

class SQLitePool:
    """
    Connection pool для SQLite.
    1 connection per thread
    """
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._local = threading.local()
        SQLiteAdapters.register()

    def get(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.db_path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                check_same_thread=False,
            )

            conn.row_factory = sqlite3.Row
            self._init_pragmas(conn)
            self._local.conn = conn

        return conn

    def close(self):
        conn = getattr(self._local, "conn", None)

        if conn:
            conn.close()
            self._local.conn = None

    @staticmethod
    def _init_pragmas(conn):
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-100000")
        conn.execute("PRAGMA mmap_size=30000000000")