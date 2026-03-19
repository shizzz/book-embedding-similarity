import asyncio
from app.workers import GenerateTags
from app.infrastructure.models.constants import SearchIndexLevel

def run(args):
    worker = GenerateTags()
    asyncio.run(worker.run())