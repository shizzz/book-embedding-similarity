import asyncio
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
)

from settings import MAX_WORKERS

class StatsUI:
    def __init__(self, max_workers: int = MAX_WORKERS):
        self.max_workers = max_workers

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

        self.progress = Progress(
            TextColumn("[bold cyan]Book analysis progress"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total} books"),
            TextColumn("{task.speed} books/s"),
            TimeRemainingColumn(),
        )

        self.progress_task = self.progress.add_task(
            "[bold green]Books processed",
            total=0,
        )

    # ---------- rendering ----------

    def _make_table(self) -> Table:
        table = Table(title="WS Library Scanner", expand=True)
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
        grid.add_row(self.progress)
        return grid

    # ---------- public API ----------

    async def init(self, total: int, remaining: int):
        async with self.lock:
            self.stats["Total"] = total
            self.stats["Remaining"] = remaining
            self.stats["Done"] = 0
            self.stats["Errors"] = 0

        self.progress.update(self.progress_task, total=remaining)

    async def set_thread(self, worker_id: int, name: str, live):
        async with self.lock:
            self.stats[f"Thread {worker_id}"] = name
        live.update(self.layout())

    async def done(self, live):
        async with self.lock:
            self.stats["Done"] += 1
            self.stats["Remaining"] -= 1

        self.progress.update(self.progress_task, advance=1)
        live.update(self.layout())

    async def error(self, live):
        async with self.lock:
            self.stats["Errors"] += 1

        self.progress.update(self.progress_task, advance=1)
        live.update(self.layout())