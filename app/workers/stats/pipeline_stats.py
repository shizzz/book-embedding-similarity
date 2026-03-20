import asyncio
from app.infrastructure.models import StageStats
from .stats import Stats
from .edges import EdgeStats

class PipelineStats(Stats):
    def __init__(self):
        self.stages: dict[str, StageStats] = {}
        self.edges: dict[tuple[str, str], EdgeStats] = {}
        self._lock = asyncio.Lock()
        self._stage_locks = {}
        self._edge_locks: dict[tuple[str, str], asyncio.Lock] = {}

    def _get_stage_lock(self, name: str):
        lock = self._stage_locks.get(name)
        stage = self.stages.get(name)
        return stage, lock
    
    async def register_stage(self, name: str, workers: int = 1, queue_max_size: int = 0):
        lock = None
        async with self._lock:
            if name not in self.stages:
                # создаём новую стадию
                st = StageStats(name, workers)
                st.queue_max_size = queue_max_size
                st.start()
                self.stages[name] = st
                self._stage_locks[name] = asyncio.Lock()
            else:
                lock = self._stage_locks[name]

        if lock:
            async with lock:
                st = self.stages.get(name)
                st.workers = workers
                st.queue_max_size = queue_max_size

    async def update_stage_info(self, name: str, batch_size: int):
        st, lock = self._get_stage_lock(name)
        async with lock:
            st.batch_size = batch_size

    async def unregister_stage(self, name: str, workers: int = 1):
        async with self._lock:
            lock = self._stage_locks.get(name)

        async with lock:
            stage = self.stages.get(name)
            if not stage:
                return

            stage.workers -= workers

            if stage.workers <= 0:
                del self.stages[name]
                del self._stage_locks[name]

    async def register_edge(self, upstream: str, downstream: str):
        async with self._lock:
            key = (upstream, downstream)
            if key not in self.edges:
                self.edges[key] = EdgeStats(upstream, downstream)
                self._edge_locks[key] = asyncio.Lock()

    async def edge_dispatch(self, upstream: str, downstream: str, n=1):
        key = (upstream, downstream)
        async with self._edge_locks[key]:
            self.edges[key].inc(n)

    async def set_total(self, stage, total):
        st, lock = self._get_stage_lock(stage)
        async with lock:
            st.total = total

    async def done(self, stage, count=1):
        st, lock = self._get_stage_lock(stage)
        async with lock:
            st.processed += count

    async def queue_size(self, stage, size):
        st, lock = self._get_stage_lock(stage)
        async with lock:
            st.queue = size

    async def error(self, stage):
        st, lock = self._get_stage_lock(stage)
        async with lock:
            st.errors += 1

    async def finish(self, stage: str):
        st, lock = self._get_stage_lock(stage)
        async with lock:
            st.finished = True