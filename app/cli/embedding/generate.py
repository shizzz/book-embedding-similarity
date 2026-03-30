from app.ui import DummyUI
from app.workers.stats import Stats
from app.workers import GenerateEmbeddings

async def run(args, stats: Stats = None):
    worker = GenerateEmbeddings(
        batch=args.batch,
        stats=stats,
        ui=DummyUI() if args.disable_ui else None,
    )
    await worker.run()