from .base_batch_strategy import BaseBatchStrategy

class CharBatchStrategy(BaseBatchStrategy):
    def __init__(self, func):
        self.func = func
        self.current_chars = 0

    def on_add(self, task):
        self.current_chars += task.entity.length

    def should_flush(self, batch) -> bool:
        return self.current_chars >= self.func()

    def reset(self):
        self.current_chars = 0