import asyncio


class Channel:
    def __init__(self, downstream: str, queue: asyncio.Queue | None = None):
        self.downstream = downstream
        self.queue = queue or asyncio.Queue()

    @property
    def edge_name(self) -> str:
        return self.downstream