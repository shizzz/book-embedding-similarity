import asyncio
from asyncio import create_task, gather
from rich.live import Live
from ui import StatsUI
from db import init_db
from book import BookRegistry
from settings import MAX_WORKERS

registry = BookRegistry()
ui = StatsUI()

# ------------------ Worker ------------------
async def worker(worker_id, live, process):
    while True:
        task = registry.get_next_book()
        if task is None:
            await ui.set_thread(worker_id, live, "---")
            break

        try:
            await ui.set_thread(worker_id, live, task.file_name)

            await asyncio.to_thread(process, task)

            await ui.stat_done(live)
            registry.mark_completed(task)
        except Exception as error:
            await ui.stat_error(live)
            registry.mark_completed(task)
            print(f"ERROR processing {task.file_name}: {error}")

async def main(statProcess, workerProcess, sleepy: bool = False):
    print(f"Prepare...")

    await init_db()

    print(f"Stat books...")
    await asyncio.to_thread(statProcess)
    total, completed = registry.stats()
    remaining = total - completed
    await ui.init_stat(total, remaining)
    
    print(f"Starting processing for {remaining} books...")

    with Live(ui.layout(), refresh_per_second=5, console=ui.console) as live:
        tasks = [create_task(worker(i, live, workerProcess, sleepy)) for i in range(1, MAX_WORKERS + 1)]
        await gather(*tasks)

    print("All book processed!")
