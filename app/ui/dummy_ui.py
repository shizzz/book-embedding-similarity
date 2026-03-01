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
    
class _DummyTqdm:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False 

    def update(self, n: int = 1):
        pass

    def set_total(self, total: int):
        pass

class _DummyTqdmIterable:
    def __init__(self, iterable, *args, **kwargs):
        self.iterable = iterable

    def __iter__(self):
        for item in self.iterable:
            yield item

class DummyUI:
    def __init__(self):
        self.__dict__ = _DummyObject().__dict__

    def __getattr__(self, name):
        return _DummyObject()

    def tqdm(self, obj=None, *args, **kwargs):
        if hasattr(obj, "__iter__") and obj is not None:
            return _DummyTqdmIterable(obj)
        return _DummyTqdm(*args, **kwargs)