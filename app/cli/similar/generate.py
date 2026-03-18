import asyncio
from app.workers import GenerateSimilarWorker
from app.infrastructure.models.constants import SearchIndexLevel

def run(args):
    level = args.level or SearchIndexLevel.CHUNK
    top = args.top or 100
    exclude_same_authors = args.exclude_same_authors or True

    worker = GenerateSimilarWorker(
        level=level,
        top_k=top,
        exclude_same_authors=exclude_same_authors,
    )
    asyncio.run(worker.run())