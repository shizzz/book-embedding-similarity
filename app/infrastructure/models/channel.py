import asyncio


class Channel:
    def __init__(self, downstream: str, queue: asyncio.Queue | None = None):
        self.downstream = downstream
        self.queue = queue or asyncio.Queue()
        self.upstream_done = asyncio.Event()
        self.producers: int = 0

        self._lock = asyncio.Lock()

    @property
    def edge_name(self) -> str:
        return self.downstream
    
    async def add_upstream(self):
        async with self._lock:
            self.producers += 1

            if self.producers > 0:
                self.upstream_done.clear()

    async def done(self) -> None:
        async with self._lock:
            self.producers -= 1
            
            if self.producers <= 0:
                self.upstream_done.set()