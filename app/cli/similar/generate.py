from app.ui import DummyUI
from app.workers.stats import Stats
from app.workers import GenerateSimilarWorker
from app.infrastructure.models.constants import SearchIndexLevel

async def run(args, stats: Stats = None):
    level = args.level or SearchIndexLevel.CHUNK
    top = args.top or 100
    exclude_same_authors = args.exclude_same_authors or True

    worker = GenerateSimilarWorker(
        level=level,
        top_k=top,
        exclude_same_authors=exclude_same_authors,
        stats=stats,
        ui=DummyUI() if args.disable_ui else None,
    )
    await worker.run()