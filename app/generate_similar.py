import asyncio
from app.workers import GenerateSimilarWorker
from app.settings.config import MAX_WORKERS

if __name__ == "__main__":
    worker = GenerateSimilarWorker(max_workers = MAX_WORKERS)
    asyncio.run(worker.run())
