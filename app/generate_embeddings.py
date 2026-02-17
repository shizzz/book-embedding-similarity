import asyncio
from app.workers import GenerateEmbeddingsWorker
from app.model.model import Model

def main():
    model = Model().get()
    worker = GenerateEmbeddingsWorker(max_workers=2, model=model, title="Generate embeddings")
    asyncio.run(worker.run())

if __name__ == "__main__":
    main()