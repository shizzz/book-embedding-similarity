from dataclasses import dataclass, field
import time
from typing import Optional

@dataclass
class StageStats:
    name: str
    workers: int

    processed: int = 0
    errors: int = 0

    queue: int = 0
    total: Optional[int] = None

    started: float = field(default_factory=time.time)

    def throughput(self):
        dt = time.time() - self.started
        return self.processed / dt if dt else 0

    def progress(self):
        if self.total:
            return self.processed / self.total
        return None