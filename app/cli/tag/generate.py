import asyncio
from app.workers.stats import Stats
from app.workers import GenerateTags

def run(args, stats: Stats = None):
    worker = GenerateTags(
        centros=args.centros or 256, 
        threshold=args.trenshold or 0.0,
        recreate=args.recreate,
        stats=stats,
    )
    asyncio.run(worker.run())