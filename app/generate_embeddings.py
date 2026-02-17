import asyncio
from app.workers import GenerateEmbeddingsWorker
from app.model.model import Model

if __name__ == "__main__":
    model = Model().get()
    worker = GenerateEmbeddingsWorker(max_workers=2, model=model, title="Generate embeddings")
    asyncio.run(worker.run())
