import asyncio
from app.workers import GenerateTags

def run(args):
    worker = GenerateTags(threshold=args.trenshold)
    asyncio.run(worker.run())