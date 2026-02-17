import asyncio
from app.workers import GenerateEmbeddingsWorker

def main():
    worker = GenerateEmbeddingsWorker(max_workers=4, title="Generate embeddings")
    asyncio.run(worker.run())

if __name__ == "__main__":
    main()