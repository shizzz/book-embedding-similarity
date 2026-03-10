from .stats import Stats

class NullStats(Stats):
    def __getattr__(self, name):
        # Любой вызов метода возвращает awaitable, который ничего не делает
        async def dummy(*args, **kwargs):
            return None
        return dummy

    def __iter__(self):
        return iter([])

    def __bool__(self):
        return False