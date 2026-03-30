from app.workers.stats import Stats
from .generate import run as generate

async def run(args, stats: Stats = None):
    if args.command == "generate":
        await generate(args, stats)