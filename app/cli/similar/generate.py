import asyncio
from app.workers import GenerateSimilarWorker

def run(args):
    batch_size = int(args.batch or 10)
    db_queue_batch_size = 20000
    queue_size = 0

    worker = GenerateSimilarWorker(None)
    asyncio.run(worker.run())