import asyncio
from asyncio import create_task, gather
from rich.live import Live
from app.utils import StatsUI
from app.db import DBManager
from app.models import BookRegistry
from app.settings import MAX_WORKERS

registry = BookRegistry()
ui = StatsUI()
db = DBManager()

# ------------------ Worker ------------------
async def worker_process(worker_id: int, live: Live, process, sleepy: bool):
    while True:
        task = registry.get_next_book()
        if task is None:
            await ui.set_thread(worker_id, live, "---")
            break

        try:
            await ui.set_thread(worker_id, live, task.file_name)

            await asyncio.to_thread(process, task)

            await ui.done(live)
            registry.mark_completed(task)
        except Exception as error:
            await ui.error(live)
            registry.mark_completed(task)
            print(f"ERROR processing {task.file_name}: {error}")

async def worker_startup(statProcess, workerProcess, sleepy: bool = False):
    print(f"Prepare...")

    await asyncio.to_thread(db.init_db)

    print(f"Stat books...")
    await asyncio.to_thread(statProcess)
    total, completed = registry.stats()
    remaining = total - completed
    await ui.init(total, remaining)
    
    print(f"Starting processing for {remaining} books...")

    with Live(ui.layout(), refresh_per_second=5, console=ui.console) as live:
        tasks = [create_task(worker_process(i, live, workerProcess, sleepy)) for i in range(1, MAX_WORKERS + 1)]
        await gather(*tasks)

    print("All book processed!")
