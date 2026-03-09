class BaseBatchStrategy:

    def should_flush(self, batch) -> bool:
        """
        Возвращает True если batch нужно отправить в process
        """
        raise NotImplementedError

    def on_add(self, task):
        """
        вызывается при добавлении task в batch (опционально)
        """
        pass

    def reset(self):
        """
        сброс состояния после flush
        """
        pass