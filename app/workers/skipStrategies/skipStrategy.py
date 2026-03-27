from typing import Generic, Callable, Set, List
from app.common.types import TEntity
from app.infrastructure.models import Task
from .base_skip_strategy import BaseSkipStrategy

class SkipIfInSetStrategy(BaseSkipStrategy, Generic[TEntity]):
    """
    Пропуск элементов, если значение, полученное через key_fn, есть в множестве values_set
    """
    def __init__(
        self,
        key_fn: Callable[[TEntity], any],
        values_set: Set[any],
    ):
        """
        key_fn: функция, которая возвращает значение атрибута entity для проверки
        values_set: множество значений, которые нужно пропускать
        """
        self.key_fn = key_fn
        self.values_set = values_set

    def skip(self, item: Task[TEntity]) -> bool:
        value = self.key_fn(item.entity)
        return value in self.values_set

    def filter(self, items: List[Task[TEntity]]) -> List[Task[TEntity]]:
        return [t for t in items if not self.skip(t)]