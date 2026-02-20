import asyncio
from app.workers import GenerateSimilarWorker
from app.settings.config import MAX_WORKERS

def main():
    worker = GenerateSimilarWorker(
        max_workers = MAX_WORKERS,
        title="Generate similar", 
        queue_size=0,
        db_queue_batch_size=20000,
        db_queue_max_size=0
    )
    asyncio.run(worker.run())

if __name__ == "__main__":
    main()