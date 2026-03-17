
import asyncio
from app.settings import ProcessConfig
from app.workers import GenerateEmbeddings

def run(args):
    queue_size = ProcessConfig.MAX_WORKERS * 3

    worker = GenerateEmbeddings(
        batch=args.batch
    )
    asyncio.run(worker.run())