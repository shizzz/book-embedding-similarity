import time

class EdgeStats:
    def __init__(self, upstream: str, downstream: str):
        self.upstream = upstream
        self.downstream = downstream
        self.count = 0
        self.start = time.time()

    def inc(self, n=1):
        self.count += n

    @property
    def speed(self):
        elapsed = time.time() - self.start
        if elapsed == 0:
            return "-"
        return f"{self.count / elapsed:.2f}/s"

    def to_dict(self):
        return {"count": self.count}