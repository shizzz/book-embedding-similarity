import time
from abc import ABC
from app.infrastructure.models import StageStats
from .edges import EdgeStats

class Stats(ABC):
    stages: dict[str, StageStats] = {}
    edges: dict[tuple[str, str], EdgeStats] = {}
    start_time = time.time()
    
    async def register_stage(self, name: str, workers: int = 1, queue_max_size: int = 0):
        pass
    
    async def unregister_stage(self, name: str, workers: int = 1):
        pass

    def register_edge(self, upstream: str, downstream: str):
        pass

    def edge_dispatch(self, upstream: str, downstream: str, n=1):
        pass

    async def set_total(self, stage: str, total: int):
        pass

    async def done(self, stage: str, count: int = 1):
        pass

    async def queue_size(self, stage: str, size: int):
        pass

    async def error(self, stage: str):
        pass

    async def finish(self, stage: str):
        pass
    
    @property
    def runtime(self) -> str:
        elapsed = time.time() - self.start_time

        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)
        s = int(elapsed % 60)

        if h > 0:
            return f"{h}h {m}m {s}s"
        if m > 0:
            return f"{m}m {s}s"
        return f"{s}s"