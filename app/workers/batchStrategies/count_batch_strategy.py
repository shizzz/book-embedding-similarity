from .base_batch_strategy import BaseBatchStrategy
from app.infrastructure.models import Task

class CountBatchStrategy(BaseBatchStrategy):
    """Формирует batch по количеству элементов"""

    def __init__(self, max_items: int):
        self.max_items = max_items
        self.buffer: list = []

    def info(self):
        return str(self.max_items)

    def collect(self, task: Task) -> list[Task] | None:
        self.buffer.append(task)
        if len(self.buffer) >= self.max_items:
            batch = self.buffer
            self.buffer = []
            return batch
        return None

    def flush(self) -> list[Task] | None:
        if self.buffer:
            batch = self.buffer
            self.buffer = []
            return batch
        return None