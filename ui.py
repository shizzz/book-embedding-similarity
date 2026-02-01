import asyncio
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from settings import MAX_WORKERS

# ------------------ STATS ------------------
stats = {
    "Total": 0,
    "Remaining": 0,
    "Done": 0,
    "Errors": 0
}
for i in range(1, MAX_WORKERS + 1):
    stats[f"Thread {i}"] = "-"
stats_lock = asyncio.Lock()
console = Console()
# ------------------ UI ------------------
console = Console()

progress = Progress(
    TextColumn("[bold cyan]Book analys progress"),
    BarColumn(),
    TextColumn("{task.completed}/{task.total} books"),
    TextColumn("{task.speed} books/s"),
    TimeRemainingColumn(),
)

progress_task = progress.add_task(
    "[bold green]Books processed",
    total=stats["Remaining"]
)

def make_table():
    table = Table(title="WS Library Scanner", expand=True)
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Total", str(stats["Total"]))
    table.add_row("Remaining", str(stats["Remaining"]))
    table.add_row("Done", str(stats["Done"]))
    table.add_row("Errors", str(stats["Errors"]))
    return table

def make_info():
    info = Text()
    for i in range(1, MAX_WORKERS + 1):
        key = f"Thread {i}"
        info.append(f"{key}: {stats[key]}\n")
    return info

def layout():
    grid = Table.grid(expand=True)
    grid.add_row(make_table())
    grid.add_row(make_info())
    grid.add_row(progress)
    return grid

async def set_thread(worker_id, live, name):
    async with stats_lock:
        stats[f"Thread {worker_id}"] = name
    live.update(layout())

async def stat_done(live):
    async with stats_lock:
        stats["Done"] += 1
        stats["Remaining"] -= 1
    progress.update(progress_task, advance=1)
    live.update(layout())

async def stat_error(live):
    async with stats_lock:
        stats["Errors"] += 1
    progress.update(progress_task, advance=1)
    live.update(layout())

async def init_stat(total, remaining):
    async with stats_lock:
        stats["Total"] = total
        stats["Remaining"] = remaining
        stats["Done"] = 0
    progress.update(progress_task, total=remaining)