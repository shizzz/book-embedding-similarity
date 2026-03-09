from .base_batch_strategy import BaseBatchStrategy

class CountBatchStrategy(BaseBatchStrategy):

    def __init__(self, max_items: int):
        self.max_items = max_items

    def should_flush(self, batch) -> bool:
        return len(batch) >= self.max_items