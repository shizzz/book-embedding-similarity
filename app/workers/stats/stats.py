import asyncio
import time
from abc import ABC
from app.infrastructure.models import StageStats
from .edges import EdgeStats

class Stats(ABC):
    def __init__(self, job_id: str = None, notifier = None):
        self.job_id = job_id
        self.notifier = notifier
        self.stages: dict[str, StageStats] = {}
        self.edges: dict[tuple[str, str], EdgeStats] = {}
        self.start_time = time.time()
        self.status = None
        self.message = None
    
    async def register_stage(self, name: str, workers: int = 1, queue_max_size: int = 0):
        pass
    
    async def update_stage_info(self, name: str, batch_size: int):
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

    def to_dict(self):
        pass

    async def notify(self):
        await self.notifier(self.job_id, self.to_dict())
    
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

    def get_ordered_stages(self, mode: str = "topology"):
        """
        mode:
            - "topology" (по графу pipeline)
            - "pressure" (по узкому месту)
            - "queue" (по размеру очереди)
            - "speed" (по скорости)
        """
        stage_items = list(self.stages.items())

        if not stage_items:
            return []

        # -------------------------
        # TOPOLOGY ORDER
        # -------------------------
        if mode == "topology" and self.edges:
            from collections import defaultdict, deque

            graph = defaultdict(list)
            indegree = defaultdict(int)

            # строим граф
            for (u, v) in self.edges:
                graph[u].append(v)
                indegree[v] += 1
                if u not in indegree:
                    indegree[u] = 0

            # добавим изолированные стадии
            for name, _ in stage_items:
                if name not in indegree:
                    indegree[name] = 0

            queue = deque([n for n in indegree if indegree[n] == 0])
            order = []

            while queue:
                node = queue.popleft()
                order.append(node)

                for nei in graph[node]:
                    indegree[nei] -= 1
                    if indegree[nei] == 0:
                        queue.append(nei)

            stage_map = dict(stage_items)

            # сохраняем порядок + добавляем пропущенные
            ordered = [(name, stage_map[name]) for name in order if name in stage_map]

            missing = [item for item in stage_items if item[0] not in order]
            ordered.extend(missing)

            return ordered

        # -------------------------
        # PRESSURE ORDER
        # -------------------------
        if mode == "pressure":
            return sorted(
                stage_items,
                key=lambda x: (
                    x[1].finished,
                    -(x[1].queue / (x[1].speed_value or 1e-9))
                )
            )

        # -------------------------
        # QUEUE ORDER
        # -------------------------
        if mode == "queue":
            return sorted(
                stage_items,
                key=lambda x: (
                    x[1].finished,
                    -x[1].queue
                )
            )

        # -------------------------
        # SPEED ORDER
        # -------------------------
        if mode == "speed":
            return sorted(
                stage_items,
                key=lambda x: (
                    x[1].finished,
                    x[1].speed_value
                )
            )

        # fallback
        return stage_items
    
    def atqdm(self, iterable, total: int, desc: str):
        stats = self

        class _ATqdm:
            async def __aiter__(self_inner):
                await stats.register_stage(desc)
                await stats.set_total(desc, total)

                try:
                    if hasattr(iterable, "__aiter__"):
                        async for item in iterable:
                            yield item
                            await stats.done(desc, self_inner._count(item))

                    else:
                        loop = asyncio.get_running_loop()
                        iterator = iter(iterable)

                        def safe_next(it):
                            try:
                                return True, next(it)
                            except StopIteration:
                                return False, None

                        while True:
                            has_item, item = await loop.run_in_executor(
                                None, safe_next, iterator
                            )

                            if not has_item:
                                break

                            yield item
                            await stats.done(desc, self_inner._count(item))

                finally:
                    await stats.unregister_stage(desc)

            def _count(self_inner, item):
                return len(item) if hasattr(item, "__len__") else 1

        return _ATqdm()