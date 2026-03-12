from app.infrastructure.models import Task

class BaseBatchStrategy:
    """Базовая стратегия для накопления batch"""

    def info(self) -> str:
        """
        Получить информацию о стратегии
        """
        raise NotImplementedError

    def collect(self, task) -> list[Task] | None:
        """
        Добавляет task в стратегию.
        Если batch готов к отправке, возвращает его (list[Task]).
        Иначе возвращает None.
        """
        raise NotImplementedError

    def flush(self) -> list[Task] | None:
        """
        Отдаёт все накопленные задачи и сбрасывает состояние.
        Если batch пустой — возвращает None
        """
        raise NotImplementedError