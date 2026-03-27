
import asyncio
from app.workers.stats import Stats
from app.workers import GenerateEmbeddings

def run(args, stats: Stats = None):
    worker = GenerateEmbeddings(
        batch=args.batch,
        stats=stats
    )
    asyncio.run(worker.run())