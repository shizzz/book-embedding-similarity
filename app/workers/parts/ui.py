import asyncio
from typing import Dict
from rich.live import Live
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    TaskID
)

from app.settings.config import MAX_WORKERS

class StatsUI:
    def __init__(self, max_workers: int = MAX_WORKERS, title: str = "Library scanner"):
        self.max_workers = max_workers
        self.live: Live = None
        
        self._label = title
        self._bars: Dict[int, Progress] = {}
        self._tasks: Dict[int, TaskID] = {}

        self.stats = {
            "Total": 0,
            "Remaining": 0,
            "Done": 0,
            "Errors": 0,
        }
        for i in range(1, max_workers + 1):
            self.stats[f"Thread {i}"] = "-"

        self.lock = asyncio.Lock()
        self.console = Console()
        self.add_progress("Прогресс анализа книг", "Книг")

    def _make_table(self) -> Table:
        table = Table(title=self._label, expand=True)
        table.add_column("Metric")
        table.add_column("Value")
        table.add_row("Total", str(self.stats["Total"]))
        table.add_row("Remaining", str(self.stats["Remaining"]))
        table.add_row("Done", str(self.stats["Done"]))
        table.add_row("Errors", str(self.stats["Errors"]))
        return table

    def _make_info(self) -> Text:
        info = Text()
        for i in range(1, self.max_workers + 1):
            key = f"Thread {i}"
            info.append(f"{key}: {self.stats[key]}\n")
        return info

    def layout(self) -> Table:
        grid = Table.grid(expand=True)
        grid.add_row(self._make_table())
        grid.add_row(self._make_info())

        for idx in self._bars:
            grid.add_row(self._bars[idx])
        return grid
    
    async def init(self):
        async with self.lock:
            self.stats["Total"] = 0
            self.stats["Remaining"] = 0
            self.stats["Done"] = 0
            self.stats["Errors"] = 0

        self._bars[0].update(self._tasks[0], total=0)

    async def set_thread(self, worker_id: int, name: str):
        async with self.lock:
            self.stats[f"Thread {worker_id}"] = name
        self.live.update(self.layout())

    async def done(self, idx: int = 0, count: int = 1):
        async with self.lock:
            if idx == 0:
                self.stats["Done"] += count
                self.stats["Remaining"] -= count

            self._bars[idx].update(task_id=self._tasks[idx], advance=count)
            self.live.update(self.layout())

    async def update_total(self, total: int, idx: int = 0):
        async with self.lock:
            if idx == 0:
                old_total = self.stats["Total"]
                delta = total - old_total

                self.stats["Total"] = total
                self.stats["Remaining"] += delta

            self._bars[idx].update(self._tasks[idx], total=total)

    async def error(self, idx: int = 0):
        async with self.lock:
            self.stats["Errors"] += 1

        self._bars[idx].update(self._tasks[idx], advance=1)
        self.live.update(self.layout())

    def add_progress(self, descr: str, unit: str) -> int:
        idx = max(self._bars.keys(), default=-1) + 1

        self._bars[idx] = Progress(  
            TextColumn(
                "[bold cyan]{task.description:>20}",
                justify="left",
            ),
            BarColumn(bar_width=40),
            TextColumn(
                "{task.completed:>6}/{task.total:<6}",
                justify="right"
            ),    
            TextColumn(
                "{task.speed} {task.fields[unit]}/с",
                justify="right",
            ),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
        )

        self._tasks[idx] = self._bars[idx].add_task(
            description=f"[bold green]{descr}",
            total=0,
            unit=unit,
        )

        return idx