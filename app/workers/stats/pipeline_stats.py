from app.infrastructure.models import StageStats
from .stats import Stats
from .edges import EdgeStats

class PipelineStats(Stats):
    def __init__(self):
        self.stages: dict[str, StageStats] = {}
        self.edges: dict[tuple[str, str], EdgeStats] = {}

    async def register_stage(self, name: str, workers: int = 1):
        st = StageStats(name, workers)
        st.start()
        self.stages[name] = st

    async def unregister_stage(self, name: str, workers: int = 1):
        stage = self.stages.get(name)
        if not stage:
            return

        stage.workers -= workers

        if stage.workers <= 0:
            del self.stages[name]

    async def register_edge(self, upstream: str, downstream: str):
        key = (upstream, downstream)
        if key not in self.edges:
            self.edges[key] = EdgeStats(upstream, downstream)

    async def edge_dispatch(self, upstream: str, downstream: str, n=1):
        self.edges[(upstream, downstream)].inc(n)

    async def set_total(self, stage, total):
        self.stages[stage].total = total

    async def done(self, stage, count=1):
        self.stages[stage].processed += count

    async def queue_size(self, stage, size):
        self.stages[stage].queue = size

    async def error(self, stage):
        self.stages[stage].errors += 1