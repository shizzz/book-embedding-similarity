from app.ui import DummyUI
from app.workers.stats import Stats
from app.workers import GenerateTags

async def run(args, stats: Stats = None):
    worker = GenerateTags(
        centros=args.centros or 256, 
        threshold=args.threshold or 0.0,
        recreate=args.recreate,
        stats=stats,
        ui=DummyUI() if args.disable_ui else None,
    )
    await worker.run()