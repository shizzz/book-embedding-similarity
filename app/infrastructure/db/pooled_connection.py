import asyncio
from .pool import SQLitePool

class PooledConnection:
    def __init__(self, pool: SQLitePool, autocommit=True):
        self.pool = pool
        self.autocommit = autocommit
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.get()
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            if self.autocommit:
                self.conn.commit()
        else:
            self.conn.rollback()

class PooledConnectionAsync:
    def __init__(self, *, lock: asyncio.Lock, pooled: PooledConnection):
        self._lock = lock
        self._pooled = pooled
        self._conn = None

    async def __aenter__(self):
        await self._lock.acquire()
        try:
            self._conn = self._pooled.__enter__()
            return self._conn
        except Exception:
            self._lock.release()
            raise

    async def __aexit__(self, exc_type, exc, tb):
        try:
            return self._pooled.__exit__(exc_type, exc, tb)
        finally:
            self._lock.release()