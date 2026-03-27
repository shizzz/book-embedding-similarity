import asyncio
from app.workers.stats import Stats
from app.ui import DummyUI
from app.workers import GenerateTags

def run(args, stats: Stats = None):
    worker = GenerateTags(
        centros=args.centros or 256, 
        threshold=args.threshold or 0.0,
        recreate=args.recreate,
        stats=stats,
    )
    asyncio.run(worker.run())

async def async_run(args, stats: Stats = None):
    worker = GenerateTags(
        centros=args.centros or 256, 
        threshold=args.threshold or 0.0,
        recreate=args.recreate,
        stats=stats,
        ui=DummyUI()
    )
    await worker.run()