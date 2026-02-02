import asyncio
from asyncio import create_task, gather
from rich.live import Live
from app.utils import StatsUI
from app.db import DBManager
from app.models import BookRegistry
from app.settings.config import MAX_WORKERS

class BaseWorker:
    def __init__(
        self,
        registry: BookRegistry = None,
        max_workers: int = MAX_WORKERS,
        show_ui: bool = True,
        sleepy: bool = False
    ):
        """
        stat_process: функция для подготовки реестра (statBooks)
        worker_process: функция обработки одной книги (process_book)
        registry: BookRegistry (если None, создается новый)
        max_workers: количество параллельных воркеров
        sleepy: bool, использовать задержки
        """
        self.registry = registry or BookRegistry()
        self.db = DBManager()
        self.max_workers = max_workers
        self.sleepy = sleepy
        self.show_ui = show_ui

        if self.show_ui:
            self.ui = StatsUI()
        
    def stat_books(self):
        raise NotImplementedError("stat_books must be implemented by subclass")

    def process_book(self, task):
        raise NotImplementedError("process_book must be implemented by subclass")
    
    async def _worker(self, worker_id: int, live: Live):
        while True:
            task = self.registry.get_next_book()
            if task is None:
                if self.sleepy:
                    asyncio.sleep(1)
                else:
                    if self.show_ui:
                        await self.ui.set_thread(worker_id, live, "---")
                    break

            try:
                if self.show_ui:
                    await self.ui.set_thread(worker_id, live, task.file_name)

                await asyncio.to_thread(self.process_book, task)

                if self.show_ui:
                    await self.ui.done(live)
                self.registry.mark_completed(task)
            except Exception as error:
                if self.show_ui:
                    await self.ui.error(live)
                self.registry.mark_completed(task)
                print(f"ERROR processing {task.file_name}: {error}")

    async def run(self):
        print("Prepare...")

        # инициализация базы
        await asyncio.to_thread(self.db.init_db)

        # подготовка реестра
        print("Stat books...")
        await asyncio.to_thread(self.stat_books)

        total, completed = self.registry.stats()
        remaining = total - completed
        if self.show_ui:
            await self.ui.init(total, remaining)

        print(f"Starting processing for {remaining} books...")

        if self.show_ui:
            with Live(self.ui.layout(), refresh_per_second=5, console=self.ui.console) as live:
                tasks = [
                    create_task(self._worker(i, live))
                    for i in range(1, self.max_workers + 1)
                ]
                await gather(*tasks)
        else:
            tasks = [
                create_task(self._worker(i, None))
                for i in range(1, self.max_workers + 1)
            ]
            await gather(*tasks)


        print("All books processed!")
