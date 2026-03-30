from app.ui import DummyUI
from app.workers.stats import Stats
from app.workers import GenerateIndexWorker

async def run(args, stats: Stats = None):
    level = args.level

    worker = GenerateIndexWorker(
        level=level,
        stats=stats,
        ui=DummyUI() if args.disable_ui else None,
    )
    await worker.run()