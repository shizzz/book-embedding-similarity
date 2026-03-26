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
        self.queue_max_size: int = 0
        self.workers = workers
        self.start_time: float | None = None
        self.finished: bool = False
        self.speed_value: float = 0
        self.batch_size: str = None
        self._eta_value: str = "-"

    def start(self):
        self.start_time = time.time()

    def finish(self):
        _ = self.speed
        _ = self.eta
        self.finished = True

    @property
    def percent(self) -> str:
        if self.total:
            p = int(self.processed / self.total * 100)
            return f"{p}%"
        return "N/A"

    @property
    def speed(self) -> str:
        if self.finished:
            return f"{self.speed_value:.2f}/s" if self.speed_value else "-"

        if not self.start_time:
            return "-"
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            self.speed_value = 0
            return "-"

        self.speed_value = self.processed / elapsed
        return f"{self.speed_value:.2f}/s"

    @property
    def eta(self) -> str:
        if self.finished:
            return self._eta_value

        if self.total is None or not self.start_time:
            return "-"

        remaining = self.total - self.processed
        elapsed = time.time() - self.start_time

        if self.processed == 0 or elapsed == 0:
            return "-"

        rate = self.processed / elapsed
        remaining_seconds = int(remaining / rate)

        days, rem = divmod(remaining_seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, seconds = divmod(rem, 60)

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0 or days > 0:
            parts.append(f"{hours}h")
        if minutes > 0 or hours > 0 or days > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{seconds}s")

        self._eta_value = " ".join(parts)
        return self._eta_value