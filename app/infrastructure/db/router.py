import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, AsyncIterator
from .pool import SQLitePool
from .pooled_connection import PooledConnection, PooledConnectionAsync
from .transaction import DBTransaction, TransactionAsync
from app.settings import PathsConfig

class DBRouter:
    def __init__(self):
        self.base_dir = PathsConfig.DATA_DIR
        self.base_dir.mkdir(exist_ok=True)
        self._pools: Dict[Path, SQLitePool] = {}
        # asyncio.Lock per database file path
        self._locks: Dict[Path, asyncio.Lock] = {}

    def _get_pool(self, path: Path) -> SQLitePool:
        if path not in self._pools:
            self._pools[path] = SQLitePool(path)
        return self._pools[path]

    def _get_lock(self, path: Path) -> asyncio.Lock:
        lock = self._locks.get(path)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[path] = lock
        return lock

    def transaction(self) -> DBTransaction:
        return DBTransaction(self)

    def transaction_async(self, model_uid: str | None = None) -> TransactionAsync:
        return TransactionAsync(router=self, model_uid=model_uid)

    def meta(self) -> PooledConnection:
        path = self.base_dir / "meta.db"
        return PooledConnection(self._get_pool(path))

    def meta_async(self) -> PooledConnectionAsync:
        return PooledConnectionAsync(lock=self.meta_lock(), pooled=self.meta())

    def meta_lock(self) -> asyncio.Lock:
        path = self.base_dir / "meta.db"
        return self._get_lock(path)

    def chunks(self) -> PooledConnection:
        path = self.base_dir / "chunks.db"
        return PooledConnection(self._get_pool(path))

    def chunks_async(self) -> PooledConnectionAsync:
        return PooledConnectionAsync(lock=self.chunks_lock(), pooled=self.chunks())

    def chunks_lock(self) -> asyncio.Lock:
        path = self.base_dir / "chunks.db"
        return self._get_lock(path)

    def embeddings(self, model_uid) -> PooledConnection:
        path = self.base_dir / f"embeddings_{model_uid}.db"
        return PooledConnection(self._get_pool(path))

    def embeddings_async(self, model_uid) -> PooledConnectionAsync:
        return PooledConnectionAsync(lock=self.embeddings_lock(model_uid), pooled=self.embeddings(model_uid))

    def embeddings_lock(self, model_uid) -> asyncio.Lock:
        path = self.base_dir / f"embeddings_{model_uid}.db"
        return self._get_lock(path)

    @asynccontextmanager
    async def lock_all(self, model_uid: str | None = None) -> AsyncIterator[None]:
        """
        Acquire locks for all databases (meta, chunks, and optionally embeddings for a model)
        in a single async context to coordinate concurrent access.
        """
        locks: list[asyncio.Lock] = [
            self.meta_lock(),
            self.chunks_lock(),
        ]
        if model_uid is not None:
            locks.append(self.embeddings_lock(model_uid))

        # Acquire in fixed order to avoid deadlocks
        for lock in locks:
            await lock.acquire()

        try:
            yield
        finally:
            # Release in reverse order
            for lock in reversed(locks):
                lock.release()

    def close_all(self):
        for pool in self._pools.values():
            pool.close()