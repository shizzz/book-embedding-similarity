import asyncio
from app.workers import GenerateSimilarWorker

if __name__ == "__main__":
    worker = GenerateSimilarWorker()
    asyncio.run(worker.run())
