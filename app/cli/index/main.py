from app.workers.stats import Stats
from .generate import run as generate
from .learn import run as learn

async def run(args, stats: Stats = None):
    if args.command == "generate":
        await generate(args, stats)
    if args.command == "learn":
        await learn(args, stats)