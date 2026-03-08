from dataclasses import dataclass
import time

@dataclass
class StageStats:
    def __init__(self, name: str, workers: int):
        self.name = name
        self.total: int | None = None
        self.processed: int = 0
        self.errors: int = 0
        self.queue: int = 0
        self.workers = workers
        self.start_time: float | None = None

    def start(self):
        self.start_time = time.time()

    @property
    def percent(self) -> str:
        if self.total:
            p = int(self.processed / self.total * 100)
            return f"{p}%"
        return "N/A"

    @property
    def speed(self) -> str:
        if not self.start_time:
            return "-"
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return "-"
        return f"{self.processed / elapsed:.2f}/s"

    @property
    def eta(self) -> str:
        if self.total is None or not self.start_time:
            return "-"
        remaining = self.total - self.processed
        elapsed = time.time() - self.start_time
        if self.processed == 0 or elapsed == 0:
            return "-"
        rate = self.processed / elapsed
        return f"{remaining / rate:.1f}s"