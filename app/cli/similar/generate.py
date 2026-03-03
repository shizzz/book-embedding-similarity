import asyncio
from app.settings.config import MAX_WORKERS
from app.workers import GenerateSimilarWorker

def run(args):
    batch_size = int(args.batch or 10)
    db_queue_batch_size = 20000
    queue_size = 0

    worker = GenerateSimilarWorker(
        max_workers = MAX_WORKERS,
        title="Generate similar", 
        batch_size=batch_size,
        queue_size=queue_size,
        db_queue_batch_size=db_queue_batch_size,
        db_queue_max_size=queue_size
    )
    asyncio.run(worker.run())