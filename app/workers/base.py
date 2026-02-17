import asyncio
import logging
import traceback
from asyncio import create_task, gather
from rich.live import Live
from app.utils import StatsUI
from app.db import Migrator
from app.settings.config import MAX_WORKERS

class BaseWorker:
    def __init__(
        self,
        max_workers: int = MAX_WORKERS,
        show_ui: bool = True,
        sleepy: bool = False,
        title: str = None,
    ):
        self.queue = asyncio.Queue()
        self.max_workers = max_workers
        self.sleepy = sleepy
        self.show_ui = show_ui
        self._queue_pulled = False

        if self.show_ui:
            self.ui = StatsUI(max_workers=self.max_workers, title=title)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s %(message)s'))
        self.logger.addHandler(handler)
        
    async def stat_books(self):
        raise NotImplementedError("stat_books must be implemented by subclass")

    async def get_total(self) -> int:
        return 0
    
    async def pull_queue(self):
        self._queue_pulled = True

    async def process_book(self, task):
        raise NotImplementedError("process_book must be implemented by subclass")

    async def fin(self):
        self.logger.info(f"Nothing to finalise")
    
    async def run(self):
        self.logger.info("Prepare...")

        # инициализация базы
        await asyncio.to_thread(Migrator().apply_schema)

        # подготовка реестра
        self.logger.info("Stat books...")
        await self.stat_books()

        if self.show_ui:
            await self.ui.init()
            with Live(self.ui.layout(), refresh_per_second=1, console=self.ui.console) as live:
                self.ui.live = live
                await self._bottom()
        else:
            await self._bottom()
    
    async def _bottom(self):
        asyncio.create_task(self._get_total())

        await self._executeWorkers()
        self.ui.console.log("Finalise")
        await self.fin()

        self.ui.console.log("All books processed!")

    async def _get_total(self):
        total = await self.get_total()
        
        if self.show_ui:
            await self.ui.update_total(total)

    async def _sleepyWorker(self):
        while True:
            try:
                await self.process_book(None)
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

                done_count = await self.process_book(task)

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
