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