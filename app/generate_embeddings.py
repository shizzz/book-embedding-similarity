import asyncio
from sentence_transformers import SentenceTransformer
from app.workers import GenerateEmbeddingsWorker
from app.settings.config import MODEL_NAME

if __name__ == "__main__":
    model = SentenceTransformer(MODEL_NAME)
    worker = GenerateEmbeddingsWorker(model=model)
    asyncio.run(worker.run())
