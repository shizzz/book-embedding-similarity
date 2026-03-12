
import asyncio
from app.workers import GenerateIndexWorker

def run(args):
    worker = GenerateIndexWorker()
    asyncio.run(worker.run())