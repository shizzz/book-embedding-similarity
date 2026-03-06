import asyncio
from abc import ABC
from typing import Optional, Generic, List
from app.workers.stats import Stats, NullStats
from app.common.types import TEntity
from app.infrastructure.models import Task, TaskResult

class BaseQueueWorker(ABC, Generic[TEntity]):
    """
    Stage worker с очередью, fan-out, batching и shutdown
    """
    def __init__(
        self,
        input_queue: Optional[asyncio.Queue] = None,
        output_queues: Optional[List[asyncio.Queue]] = None,
        stats: Stats = NullStats(),
        batch_size: int = 1,
        name: str = "Stage",
        edge: str = "Done"
    ):
        self.stats = stats
        self.name = name
        self.edge = edge
        self.input_queue = input_queue or asyncio.Queue(batch_size)
        self.output_queues = output_queues or []
        self.batch_size = batch_size

        self._workers: List[asyncio.Task] = []
        self._producer_done = asyncio.Event()
        self._flush_lock = asyncio.Lock()

    async def start(self, max_workers: int):
        await self.stats.register_stage(self.name, self.workers)
        # старт producer если нет input
        if not self.has_input():
            asyncio.create_task(self._produce())

        # старт worker'ов
        for i in range(max_workers):
            self._workers.append(asyncio.create_task(self._worker(i)))

    async def wait(self):
        # ждём producer
        await self._producer_done.wait()
        await self.input_queue.join()
        await self._flush()
        # отменяем workers
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)

    def has_input(self) -> bool:
        """Есть ли input queue"""
        return False

    async def _produce(self):
        """Для stages без input"""
        async for task in self.produce():
            await self.input_queue.put(task)
        self._producer_done.set()

    async def _worker(self, wid: int):
        buffer: List[Task] = []

        while True:
            try:
                task = await self.input_queue.get()
            except asyncio.CancelledError:
                break

            try:
                buffer.append(task)

                if len(buffer) >= self.batch_size:
                    await self._process_batch(buffer)
                    buffer.clear()
            finally:
                self.input_queue.task_done()

        # flush оставшиеся batch-и при shutdown
        if buffer:
            await self._process_batch(buffer)

    async def _process_batch(self, batch: List[Task]):
        results = await self.process(batch)

        if not results:
            return

        await self.stats.task_done(self.name, len(batch))
        for r in results:
            await self.post_process(r)
            await self.dispatch(r)

    async def _flush(self):
        async with self._flush_lock:
            pass

    # -------------------------------
    # Методы, которые нужно реализовать
    # -------------------------------
    async def produce(self):
        """Для stages без input queue"""
        yield  # async generator

    async def process(self, batch: List[Task]) -> List[TaskResult]:
        return [b.to_result() for b in batch]

    async def post_process(self, result: TaskResult):
        """Опционально сохраняем в БД"""
        pass

    async def dispatch(self, result: TaskResult):
        await self.stats.register_edge(self.name, self.edge)
        for q in self.output_queues:
            await q.put(result.clone())