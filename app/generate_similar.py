import asyncio
from app.workers import GenerateSimilarWorker
from app.settings.config import MAX_WORKERS

def main():
    worker = GenerateSimilarWorker(max_workers = MAX_WORKERS, title="Generate similar")
    asyncio.run(worker.run())

if __name__ == "__main__":
    main()