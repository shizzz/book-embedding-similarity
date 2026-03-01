import asyncio
from app.settings.config import MAX_WORKERS

def run(args):
    if args.embedding:
        from app.workers import GenerateEmbeddingsWorker
        
        batch_value = None
        if "=" in args.embedding:
            key, val = args.embedding.split("=", 1)
            if key == "batch":
                batch_value = int(val)

        batch_size = batch_value or 10
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
    if args.similar:
        from app.workers import GenerateSimilarWorker
        
        batch_value = None
        if "=" in args.embedding:
            key, val = args.embedding.split("=", 1)
            if key == "batch":
                batch_value = int(val)

        batch_size = batch_value or 10
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