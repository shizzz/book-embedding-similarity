from .base_batch_strategy import BaseBatchStrategy
from app.infrastructure.models import Task, Embedding

class BookEmbeddingBatchStrategy(BaseBatchStrategy):
    """Формирует batch по book_id. Batch для одной книги."""

    def __init__(self):
        self.current_book_id = None
        self.buffer: list = []

    def info(self):
        return str(self.current_book_id)

    def collect(self, task: Task[Embedding]) -> list[Task] | None:
        """
        Добавляет task в стратегию.
        Если пришла задача для новой книги — отдаёт предыдущий batch.
        """
        if not self.buffer:
            self.current_book_id = task.entity.book_id
            self.buffer.append(task)
            return None

        if task.entity.book_id != self.current_book_id:
            batch = self.buffer
            self.buffer = [task]
            self.current_book_id = task.entity.book_id
            return batch

        self.buffer.append(task)
        return None

    def flush(self) -> list[Task] | None:
        """Отдаёт оставшийся batch, если он есть"""
        if self.buffer:
            batch = self.buffer
            self.buffer = []
            self.current_book_id = None
            return batch
        return None