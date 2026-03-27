from abc import ABC, abstractmethod
from typing import List
from app.infrastructure.models import Task

class BaseSkipStrategy(ABC):
    """Базовая стратегия для пропуска элементов"""

    @abstractmethod
    def skip(self, item: Task) -> bool:
        """
        Возвращает True, если элемент нужно пропустить
        """
        pass

    def filter(self, items: List[Task]) -> List[Task]:
        """Фильтруем список через skip"""
        return [t for t in items if not self.skip(t)]