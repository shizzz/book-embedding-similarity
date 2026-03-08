from abc import ABC
from app.infrastructure.models import StageStats

class Stats(ABC):
    stages: dict[str, StageStats] = {}
    edges: set[tuple[str, str]] = set()
    
    async def register_stage(self, name: str, workers: int):
        pass

    async def register_edge(self, src: str, dst: str):
        pass

    async def set_total(self, stage: str, total: int):
        pass

    async def task_done(self, stage: str, count: int = 1):
        pass

    async def queue_size(self, stage: str, size: int):
        pass

    async def task_error(self, stage: str):
        pass