from app.infrastructure.models import StageStats
from stats import Stats

class PipelineStats(Stats):
    def __init__(self):
        self.stages: dict[str, StageStats] = {}
        self.edges: set[tuple[str, str]] = set()

    async def register_stage(self, name, workers):
        self.stages[name] = StageStats(name, workers)

    async def register_edge(self, src, dst):
        self.edges.add((src, dst))

    async def set_total(self, stage, total):
        self.stages[stage].total = total

    async def task_done(self, stage, count=1):
        self.stages[stage].processed += count

    async def queue_size(self, stage, size):
        self.stages[stage].queue = size

    async def task_error(self, stage):
        self.stages[stage].errors += 1