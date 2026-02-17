import asyncio
import logging
import traceback
from abc import ABC, abstractmethod
from typing import Generic, Tuple
from asyncio import create_task, gather
from rich.live import Live
from app.workers.parts import StatsUI
from app.db import Migrator
from app.common.types import TEntity, TResult
from app.models import Task
from app.settings.config import MAX_WORKERS, DATABASE_QUEUE_BATCH_SIZE

class BaseWorker(ABC, Generic[TEntity]):
    def __init__(
        self,
        max_workers: int = MAX_WORKERS,
        show_ui: bool = True,
        sleepy: bool = False,
        title: str = None,
    ):
        self.queue: asyncio.Queue[Task[TEntity]] = asyncio.Queue()
        self.max_workers: int = max_workers
        self.sleepy: bool = sleepy
        self.show_ui: bool = show_ui
        self._queue_pulled: bool = False

        self._db_save_enabled: bool = False
        self._db_queue_total: int = 0
        self._db_queue_progress_idx: int = None
        self._db_queue: asyncio.Queue[TEntity] = asyncio.Queue()
        self._db_queue_batch_size: int = DATABASE_QUEUE_BATCH_SIZE
        self._db_queue_stop_event = asyncio.Event()
        self._db_queue_task: asyncio.Task | None = None
        self._get_total_task: asyncio.Task | None = None

        if self.show_ui:
            self.ui = StatsUI(max_workers=self.max_workers, title=title)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s %(message)s'))
        self.logger.addHandler(handler)

    @abstractmethod
    async def process(self, task: Task) -> Tuple[int, TResult]:
        raise NotImplementedError("process_book must be implemented by subclass")
        
    @abstractmethod
    async def prepare(self) -> None:
        pass

    @abstractmethod
    async def get_total(self) -> int:
        pass

    @abstractmethod
    async def fin(self) -> None:
        pass
    
    async def pull_queue(self) -> None:
        self._queue_pulled = True
    
    def save_to_db(self, buffer: TEntity) -> int:
        pass
    
    async def run(self):
        self.logger.info("Prepare...")
        self._db_save_enabled = self._is_overridden("save_to_db")

        # инициализация базы
        await asyncio.to_thread(Migrator().apply_schema)

        # подготовка реестра
        self.logger.info("Stat books...")
        await self.prepare()

        if self.show_ui:
            await self.ui.init()
            with Live(self.ui.layout(), refresh_per_second=1, console=self.ui.console) as live:
                self.ui.live = live
                await self._bottom()
        else:
            await self._bottom()
    
    def _is_overridden(self, method_name: str) -> bool:
        """
        Проверяет, переопределён ли метод в наследнике
        """
        base_method = getattr(BaseWorker, method_name)
        sub_method = getattr(self.__class__, method_name)
        return sub_method is not base_method
    
    async def _bottom(self):
        self._get_total_task = asyncio.create_task(self._get_total())

        if self._db_save_enabled:
            self.ui.console.log("DB save enabled")
            self._db_queue_progress_idx = self.ui.add_progress("Сохранение в БД", "пакетов")
            self._db_queue_task = asyncio.create_task(self._save_loop())
        else:
            self.ui.console.log("DB save disabled")

        await self._executeWorkers()
        await self._fin()

        self.ui.console.log("All books processed!")

    async def _fin(self):
        self.ui.console.log("Finalise")

        self._db_queue_stop_event.set()
        
        if self._db_save_enabled:
            db_queue_size = self._db_queue.qsize()
            if db_queue_size > 0:
                self.ui.console.log(f"Still have {db_queue_size} records to save into database")

            if self._db_queue_task:
                await self._db_queue_task

        await self.fin()

    async def _get_total(self):
        total = await self.get_total()
        
        if self.show_ui:
            await self.ui.update_total(total)

    async def _sleepyWorker(self):
        while True:
            try:
                await self.process(None)
            except Exception as error:
                self.logger.error(f"ERROR processing task: {error}")

            await asyncio.sleep(1)

    async def _worker(self, worker_id: int):
        while not self._queue_pulled or not self.queue.empty():
            try:
                task = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(1)
                continue

            try:
                if self.show_ui:
                    await self.ui.set_thread(worker_id, task.name)

                done_count, result = await self.process(task)
                
                if self._db_save_enabled:
                    self._db_queue_total += len(result)
                    await self._db_queue.put(result)
                    await self.ui.update_total(
                        idx=self._db_queue_progress_idx,
                        total=self._db_queue_total
                    )

                if self.show_ui:
                    await self.ui.done(count=done_count or 1)
            except Exception as error:
                if self.show_ui:
                    await self.ui.error()
                traceback.print_exc()
                self.ui.console.log(f"ERROR processing {task.name}: {error}")
            finally:
                self.queue.task_done()

    async def _createWorker(self, worker_id: int):
        if self.sleepy:
            await self._sleepyWorker()
        else:
            await self._worker(worker_id)

    async def _executeWorkers(self):
        tasks = []
        tasks.append(create_task(self.pull_queue()))
        tasks.extend(
            create_task(self._createWorker(i))
            for i in range(1, self.max_workers + 1)
        )
        await gather(*tasks)

    async def _db_queue_step(self, buffer: TEntity, save_if_empty: bool = False):
        try:
            item = self._db_queue.get_nowait()

            buffer.extend(item)
            self._db_queue.task_done()

            if len(buffer) >= self._db_queue_batch_size:
                done = await asyncio.to_thread(self.save_to_db, buffer)
                await self.ui.done(self._db_queue_progress_idx, done)
                buffer.clear()

            return True
        except asyncio.QueueEmpty:
            if buffer and save_if_empty:
                done = await asyncio.to_thread(self.save_to_db, buffer)
                await self.ui.done(self._db_queue_progress_idx, done)
                buffer.clear()
            await asyncio.sleep(1)
            return False
        except Exception as e:
            traceback.print_exc()
            self.ui.console.log(f"Критическая ошибка при сохранение в базу данных: {e}")
            return False

    async def _save_loop(self):
        buffer = []

        while not self._db_queue_stop_event.is_set():
            await self._db_queue_step(buffer, False)

        while await self._db_queue_step(buffer, True):
            pass

        self.ui.console.log("Save thread stopped")
