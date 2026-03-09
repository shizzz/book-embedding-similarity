import asyncio
import logging
from abc import ABC
from typing import Optional, Generic, List
from app.workers.stats import Stats, NullStats
from app.workers.batchStrategies import CountBatchStrategy
from app.common.types import TEntity
from app.infrastructure.models import Task, Channel

class BaseQueueWorker(ABC, Generic[TEntity]):
    """
    Stage worker с очередью, fan-out, batching и shutdown
    """
    def __init__(
        self,
        input_channel: Optional[Channel] = None,
        output_channels: Optional[List[Channel]] = None,
        stats: Stats = NullStats(),
        batch_size: int = 1,
        name: str = "Stage",
        producer_done = asyncio.Event(),
        workers: int = 1,
        logger: logging.Logger = None
    ):
        self.stats = stats
        self.name = name
        self.done = asyncio.Event()
        self.output_channels = output_channels or []
        self.batch_size = batch_size

        self._has_input = input_channel is not None
        self._workers_count = workers
        self._workers: List[asyncio.Task] = []
        self._producer_done = producer_done
        self._flush_lock = asyncio.Lock()

        self.input_queue = input_channel.queue if input_channel else asyncio.Queue(100)
        self.logger = logger or self.get_logger(self.name)

        self.batch_strategy = CountBatchStrategy(self.batch_size)

    async def start(self):
        await self.stats.register_stage(self.name, self._workers_count)

        edges = {ch.edge_name for ch in self.output_channels}
        if not edges:
            await self.stats.register_edge(self.name, "Done")
        else:
            for e in edges:
                await self.stats.register_edge(self.name, e)

        # старт producer если нет input
        if not self.has_input():
            asyncio.create_task(self._produce())

        # старт worker'ов
        for i in range(self._workers_count):
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
        self.done.set()

    def has_input(self) -> bool:
        """Есть ли input queue"""
        return self._has_input

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
                await self.stats.queue_size(self.name, self.input_queue.qsize())
            except asyncio.CancelledError:
                break

            try:
                buffer.append(task)
                self.batch_strategy.on_add(task)

                if self._producer_done.is_set() and self.input_queue.empty():
                    break

                if self.batch_strategy.should_flush(buffer):
                    await self._process_batch(buffer, wid)
                    buffer.clear()
                    self.batch_strategy.reset()
            except Exception as e:
                await self.stats.error(self.name)
                self.logger.exception(e)
            finally:
                self.input_queue.task_done()

        # flush оставшиеся batch-и при shutdown
        if buffer:
            await self._process_batch(buffer, wid)

    async def _process_batch(self, batch: List[Task], wid: int):
        results = await self.process(batch, wid)

        if not results:
            return

        total_done = sum(r.done for r in results)
        await self.stats.done(self.name, total_done)
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

    async def process(self, batch: List[Task], wid: int) -> List[Task]:
        return batch

    async def post_process(self, result: Task):
        """Опционально сохраняем в БД"""
        pass

    def route(self, item, channels: list[Channel]) -> list[Channel]:
        return channels

    async def dispatch(self, result: Task):
        targets = self.route(result, self.output_channels)
        for channel in targets:
            await self.stats.queue_size(channel.downstream, channel.queue.qsize())
            await self.stats.edge_dispatch(self.name, channel.downstream)
            await channel.queue.put(result.clone())

    def get_logger(self, name: str = "app") -> logging.Logger:
        logger = logging.getLogger(name)

        # если handlers уже есть — значит логгер уже настроен
        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.propagate = False

        return logger