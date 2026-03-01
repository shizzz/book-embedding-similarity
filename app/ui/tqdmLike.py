class TqdmLike:
    def __init__(self, ui, description: str, total: int = 0, unit: str = "", show_elapsed: bool = False):
        self.ui = ui
        self.description = description
        self.total = total
        self.unit = unit
        self.show_elapsed = show_elapsed
        self.idx = None
        self._closed = False

    def __enter__(self):
        self.idx = self.ui.add_progress(self.description, self.unit, self.show_elapsed)
        if self.total > 0:
            self.ui.update_total(self.total, self.idx)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._closed:
            self.ui.remove_progress(self.idx)
            self._closed = True

    def update(self, n: int = 1):
        self.ui.done(self.idx, n)

    def set_total(self, total: int):
        self.total = total
        self.ui.update_total(total, self.idx)

class TqdmIterable:
    def __init__(self, ui, iterable, description: str = "", unit: str = "", show_elapsed: bool = False):
        self.ui = ui
        self.iterable = iterable
        self.description = description
        self.unit = unit
        self.show_elapsed = show_elapsed
        self.idx = None

    def __iter__(self):
        self.idx = self.ui.add_progress(self.description, self.unit, self.show_elapsed)
        total = getattr(self.iterable, "__len__", lambda: 0)()
        if total:
            self.ui.update_total(total, self.idx)

        for item in self.iterable:
            self.ui.done(self.idx, 1)
            yield item

        self.ui.remove_progress(self.idx)