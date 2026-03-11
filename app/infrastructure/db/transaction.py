import asyncio

class DBTransaction:
    def __init__(self, router):
        self.router = router
        self._connections = []
        self._entered = False

    def __enter__(self):
        self._entered = True
        return self

    def meta(self):
        conn = self.router.meta().__enter__()
        self._connections.append(conn)
        return conn

    def chunks(self):
        conn = self.router.chunks().__enter__()
        self._connections.append(conn)
        return conn

    def embeddings(self, model_uid):
        conn = self.router.embeddings(model_uid).__enter__()
        self._connections.append(conn)
        return conn

    def commit(self):
        for conn in self._connections:
            conn.commit()

    def rollback(self):
        for conn in self._connections:
            conn.rollback()

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

class TransactionAsync:
    def __init__(self, *, router, model_uid: str | None):
        self._router = router
        self._model_uid = model_uid
        self._tx: DBTransaction | None = None
        self._locks: list[asyncio.Lock] = []

    async def __aenter__(self) -> DBTransaction:
        self._locks = [self._router.meta_lock(), self._router.chunks_lock()]
        if self._model_uid is not None:
            self._locks.append(self._router.embeddings_lock(self._model_uid))

        # Fixed order to avoid deadlocks
        for lock in self._locks:
            await lock.acquire()

        try:
            self._tx = self._router.transaction().__enter__()
            return self._tx
        except Exception:
            for lock in reversed(self._locks):
                lock.release()
            self._locks = []
            raise

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._tx is not None:
                return self._tx.__exit__(exc_type, exc, tb)
        finally:
            for lock in reversed(self._locks):
                lock.release()
            self._locks = []
            self._tx = None