from .base_batch_strategy import BaseBatchStrategy
from app.infrastructure.models import Task

class CharFuncBatchStrategy(BaseBatchStrategy):
    """Формирует batch по количеству символов (или длине entity)"""

    def __init__(self, max_chars_func):
        """
        max_chars_func — callable, возвращает лимит символов для batch
        """
        self.max_chars_func = max_chars_func
        self.buffer: list = []
        self.current_chars = 0

    def info(self):
        return str(self.max_chars_func())

    def collect(self, task: Task) -> list[Task] | None:
        self.buffer.append(task)
        self.current_chars += task.entity.length

        if self.current_chars >= self.max_chars_func():
            batch = self.buffer
            self.buffer = []
            self.current_chars = 0
            return batch
        return None

    def flush(self) -> list[Task] | None:
        if self.buffer:
            batch = self.buffer
            self.buffer = []
            self.current_chars = 0
            return batch
        return None