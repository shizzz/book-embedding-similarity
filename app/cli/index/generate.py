import asyncio
from app.workers.stats import Stats
from app.workers import GenerateIndexWorker

def run(args, stats: Stats = None):
    level = args.level

    worker = GenerateIndexWorker(
        level=level,
        stats=stats,
    )
    asyncio.run(worker.run())