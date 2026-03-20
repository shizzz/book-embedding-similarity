import asyncio
from app.workers import GenerateTags

def run(args):
    worker = GenerateTags(
        centros=args.centros or 256, 
        threshold=args.trenshold or 0.0
    )
    asyncio.run(worker.run())