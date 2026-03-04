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