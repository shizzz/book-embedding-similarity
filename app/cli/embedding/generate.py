
import asyncio
from app.settings import ProcessConfig
from app.workers import GenerateEmbeddingsWorker

def run(args):
    queue_size = ProcessConfig.MAX_WORKERS * 3

    worker = GenerateEmbeddingsWorker(
        batch=args.batch
    )
    asyncio.run(worker.run())