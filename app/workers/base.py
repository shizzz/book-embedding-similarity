import asyncio
import logging
from asyncio import create_task, gather
from rich.live import Live
from app.utils import StatsUI
from app.db import DBManager
from app.models import TaskRegistry
from app.settings.config import MAX_WORKERS

class BaseWorker:
    def __init__(
        self,
        registry: TaskRegistry = None,
        max_workers: int = MAX_WORKERS,
        show_ui: bool = True,
        sleepy: bool = False
    ):
        self.registry = registry or TaskRegistry()
        self.db = DBManager()
        self.max_workers = max_workers
        self.sleepy = sleepy
        self.show_ui = show_ui

        if self.show_ui:
            self.ui = StatsUI(max_workers = self.max_workers)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        self.logger.addHandler(handler)
        
    async def stat_books(self):
        raise NotImplementedError("stat_books must be implemented by subclass")

    def process_book(self, task):
        raise NotImplementedError("process_book must be implemented by subclass")
    
    async def fin(self):
        self.logger.info(f"Nothing to finalise")
    
    async def _sleepyWorker(self):
        while True:
            try:
                await asyncio.to_thread(self.process_book, None)
            except Exception as error:
                self.logger.error(f"ERROR processing task: {error}")

            await asyncio.sleep(1)

    async def _worker(self, worker_id: int, live: Live):
        while not self.registry.queue.empty():
            task = await self.registry.queue.get()

            try:
                if self.show_ui:
                    await self.ui.set_thread(worker_id, live, task.name)

                await asyncio.to_thread(self.process_book, task)

                if self.show_ui:
                    await self.ui.done(live)
            except Exception as error:
                if self.show_ui:
                    await self.ui.error(live)
                self.logger.error(f"ERROR processing {task.name}: {error}")
            finally:
                self.registry.queue.task_done()
                self.registry.mark_completed()

    async def _createWorker(self, worker_id: int, live: Live):
        if self.sleepy:
            await self._sleepyWorker()
        else:
            await self._worker(worker_id, live)

    async def _executeWorkers(self):
        if self.show_ui:
            with Live(self.ui.layout(), refresh_per_second=5, console=self.ui.console) as live:
                tasks = [
                    create_task(self._createWorker(i, live))
                    for i in range(1, self.max_workers + 1)
                ]
                await gather(*tasks)
        else:
            tasks = [
                create_task(self._createWorker(i, None))
                for i in range(1, self.max_workers + 1)
            ]
            await gather(*tasks)

    async def run(self):
        self.logger.info("Prepare...")

        # инициализация базы
        await asyncio.to_thread(self.db.init_db)

        # подготовка реестра
        self.logger.info("Stat books...")
        await self.stat_books()

        total = self.registry.total
        completed = self.registry.completed

        remaining = total - completed
        if self.show_ui:
            await self.ui.init(total, remaining)

        self.logger.info(f"Starting processing for {remaining} books...")
        await self._executeWorkers()
        await self.fin()

        self.logger.info("All books processed!")