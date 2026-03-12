from app.infrastructure.db.repositories import EmbeddingsRepository

class EmbeddingsBatchIterable:
    def __init__(
        self, 
        repo: EmbeddingsRepository, 
        batch_size: int = 1,
        order_by: list[str] = None,
    ):
        self.repo = repo
        self.batch_size = batch_size
        self.order_by = order_by
        self._total = None

    def __len__(self):
        if self._total is None:
            self._total = self.repo.count()
        return (self._total + self.batch_size - 1) // self.batch_size

    def __iter__(self):
        yield from self.repo.get_all_batch(self.batch_size, self.order_by)

    def count(self):
        return self.repo.count()