import asyncio
import logging
from abc import ABC
from typing import Optional, Generic, List, Callable
from app.workers.stats import Stats, NullStats
from app.workers.batchStrategies import CountBatchStrategy, BaseBatchStrategy
from app.workers.skipStrategies import BaseSkipStrategy, DummySkipStrategy
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
        workers: int = 1,
        logger: logging.Logger = None,
        batch_strategy: Optional[Callable[[], BaseBatchStrategy]] = None,
        skip_strategy: Optional[BaseSkipStrategy] = DummySkipStrategy(),
        producer_qsize: int = 10
    ):
        self.stats = stats
        self.name = name
        self.output_channels = output_channels or []
        self.batch_size = batch_size

        self._has_input = input_channel is not None
        self._workers_count = workers
        self._workers: List[asyncio.Task] = []
        self._flush_lock = asyncio.Lock()

        self.input_channel = input_channel if input_channel else Channel("self", asyncio.Queue(producer_qsize))
        self.logger = logger or self.get_logger(self.name)

        self.batch_strategy_factory = (
            batch_strategy
            if batch_strategy is not None
            else lambda: CountBatchStrategy(batch_size)
        )

        self._strategies: dict[int, BaseBatchStrategy] = {}
        self._skip_strategy = skip_strategy

    async def start(self):
        await self.stats.register_stage(self.name, self._workers_count, self.input_channel.queue.maxsize)

        for ch in self.output_channels:
             await self.stats.register_stage(ch.edge_name, 0, ch.queue.maxsize)
             await self.stats.register_edge(self.name, ch.edge_name)
             await ch.add_upstream()
             
        await self.before_start()

        # старт producer если нет input
        if not self.has_input():
            await self.input_channel.add_upstream()
            asyncio.create_task(self._produce())

        asyncio.create_task(self.count_total())
        
        # старт worker'ов
        for i in range(self._workers_count):
            self._strategies[i] = self.batch_strategy_factory()
            self._workers.append(asyncio.create_task(self._worker(i)))

        await self.stats.update_stage_info(self.name, self._strategies[i].info())

    async def wait(self):
        # ждём producer
        await self.input_channel.upstream_done.wait()
        await self.input_channel.queue.join()

        # отменяем workers
        for w in self._workers:
            w.cancel()

        await self._flush()
        await asyncio.gather(*self._workers, return_exceptions=True)

        for ch in self.output_channels:
            await ch.done()
        await self.fin()
        await self.stats.finish(self.name)
        self.logger.info(f"{self.name} worker is DONE")

    def has_input(self) -> bool:
        """Есть ли input queue"""
        return self._has_input

    async def _produce(self):
        """Для stages без input"""
        async for task in self.produce():
            await self.input_channel.queue.put(task)
            await self.stats.queue_size(self.name, self.input_channel.queue.qsize())
        await self.input_channel.done()

    async def _worker(self, wid: int):
        strategy = self._strategies[wid]

        while True:
            task = None
            try:
                task = await self.input_channel.queue.get()
            except asyncio.CancelledError:
                break

            try:
                if self._skip_strategy.skip(task):
                    await self._set_done()
                    continue
                batch = strategy.collect(task)
                if batch:
                    await self._process_batch(batch, wid)
            except Exception as e:
                await self.stats.error(self.name)
                self.logger.exception(e)
            finally:
                await self._set_done()

    async def _set_done(self):
        await self.stats.queue_size(self.name, self.input_channel.queue.qsize())
        self.input_channel.queue.task_done()

    async def _process_batch(self, batch: List[Task], wid: int):
        results = await self.process(batch, wid)

        if not results:
            return

        total_done = sum(r.done for r in results)
        await self.stats.done(self.name, total_done)
        for r in results:
            await self.post_process(r)
            await self.dispatch(r)

    async def dispatch(self, result: Task):
        targets = self.route(result, self.output_channels)
        for channel in targets:
            await self.stats.queue_size(channel.downstream, channel.queue.qsize())
            await self.stats.edge_dispatch(self.name, channel.downstream)
            await channel.queue.put(result.clone())

    async def _flush(self):
        for wid, strategy in self._strategies.items():
            batch = strategy.flush()
            if batch:
                await self._process_batch(batch, wid)

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

    async def count_total(self) -> None:
        """Опционально считаем total для stats"""
        pass
    
    async def before_start(self):
        pass

    async def fin(self):
        pass

    def route(self, item, channels: list[Channel]) -> list[Channel]:
        return channels
            
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