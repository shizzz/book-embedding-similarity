import asyncio
from app.workers import GenerateSimilarWorker

if __name__ == "__main__":
    worker = GenerateSimilarWorker(max_workers = 1)
    asyncio.run(worker.run())
