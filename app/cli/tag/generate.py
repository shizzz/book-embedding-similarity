import asyncio
from app.workers.stats import Stats
from app.workers import GenerateTags

def _w(args, stats: Stats = None):
    return GenerateTags(
        centros=args.centros or 256, 
        threshold=args.trenshold or 0.0,
        recreate=args.recreate,
        stats=stats,
    )

def run(args, stats: Stats = None):
    worker = _w(args, stats)
    asyncio.run(worker.run())

async def async_run(args, stats: Stats = None):
    worker = _w(args, stats)
    await worker.run()