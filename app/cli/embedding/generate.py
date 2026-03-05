
import asyncio
from app.settings import ProcessConfig
from app.workers import GenerateEmbeddingsWorker

def run(args):
    queue_size = ProcessConfig.MAX_WORKERS * 3

    worker = GenerateEmbeddingsWorker(
        title="Generate embeddings", 
        max_batch_size=args.batch,
        skip_embeddings=args.skip_embeddings,
        queue_size=queue_size,
        db_queue_batch_size=args.batch,
        db_queue_max_size=queue_size
    )
    asyncio.run(worker.run())