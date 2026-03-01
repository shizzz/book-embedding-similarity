class _DummyObject:
    """Любой атрибут и метод безопасны, ничего не делает"""
    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return None

    def __iter__(self):
        return iter([])

    def __bool__(self):
        return False

class DummyUI:
    """Полностью безопасный Dummy UI"""
    def __init__(self):
        self.__dict__ = _DummyObject().__dict__

    def __getattr__(self, name):
        return _DummyObject()