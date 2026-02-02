import asyncio
from app.workers import GenerateAuthorsWorker

if __name__ == "__main__":
    worker = GenerateAuthorsWorker()
    asyncio.run(worker.run())
