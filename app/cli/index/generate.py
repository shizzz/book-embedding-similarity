import asyncio
from app.workers import GenerateIndexWorker
from app.settings import IndexConfig

def run(args):
    IndexConfig.SEARCH_INDEX_LEVEL = args.level

    worker = GenerateIndexWorker(IndexConfig)
    asyncio.run(worker.run())