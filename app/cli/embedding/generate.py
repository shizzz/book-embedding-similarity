
import asyncio
from app.settings.config import MAX_WORKERS
from app.workers import GenerateEmbeddingsWorker

def run(args):
    batch_size = int(args.batch or 10)
    db_queue_batch_size = batch_size
    queue_size = batch_size * MAX_WORKERS * 3

    worker = GenerateEmbeddingsWorker(
        title="Generate embeddings", 
        max_batch_size=batch_size,
        queue_size=queue_size,
        db_queue_batch_size=db_queue_batch_size,
        db_queue_max_size=queue_size
    )
    asyncio.run(worker.run())