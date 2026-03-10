from .stats import Stats

class NullStats(Stats):
    def __getattr__(self, name):
        async def dummy(*args, **kwargs):
            return None
        return dummy

    def __iter__(self):
        return iter([])

    def __bool__(self):
        return False