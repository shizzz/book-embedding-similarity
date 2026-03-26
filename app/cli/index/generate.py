import asyncio
from app.workers import GenerateIndexWorker

def run(args):
    level = args.level

    worker = GenerateIndexWorker(level)
    asyncio.run(worker.run())