import asyncio
from app.workers import GenerateEmbeddingsWorker
from app.utils.model import Model

if __name__ == "__main__":
    model = Model().get()
    worker = GenerateEmbeddingsWorker(model=model, title="Generate embeddings")
    asyncio.run(worker.run())
