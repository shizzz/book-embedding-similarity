from pathlib import Path
from .pool import SQLitePool
from .pooled_connection import PooledConnection
from .transaction import DBTransaction
from app.settings import PathsConfig

class DBRouter:
    def __init__(self):
        self.base_dir = PathsConfig.DATA_DIR
        self.base_dir.mkdir(exist_ok=True)
        self._pools = {}

    def _get_pool(self, path: Path):
        if path not in self._pools:
            self._pools[path] = SQLitePool(path)
        return self._pools[path]

    def transaction(self):
        return DBTransaction(self)
    
    def meta(self):
        path = self.base_dir / "meta.db"
        return PooledConnection(
            self._get_pool(path)
        )

    def chunks(self):
        path = self.base_dir / "chunks.db"
        return PooledConnection(
            self._get_pool(path)
        )

    def embeddings(self, model_uid):
        path = self.base_dir / f"embeddings_{model_uid}.db"
        return PooledConnection(
            self._get_pool(path)
        )

    def close_all(self):
        for pool in self._pools.values():
            pool.close()