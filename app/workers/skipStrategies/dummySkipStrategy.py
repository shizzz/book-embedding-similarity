from app.infrastructure.models import Task
from .base_skip_strategy import BaseSkipStrategy

class DummySkipStrategy(BaseSkipStrategy):
    """
    Заглушка: не пропускает ни одного элемента
    """
    def skip(self, item: Task) -> bool:
        return False