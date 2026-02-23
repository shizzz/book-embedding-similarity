import asyncio
from app.workers import GenerateEmbeddingsWorker

def main():
    worker = GenerateEmbeddingsWorker(
        max_workers=4,
        title="Generate embeddings", 
        queue_size=50,
        db_queue_batch_size=50,
        db_queue_max_size=50)
    asyncio.run(worker.run())

if __name__ == "__main__":
    main()