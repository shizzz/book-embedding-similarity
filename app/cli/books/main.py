from app.workers.stats import Stats
from .generate import run as generate
from .anonymize import run as anonymize

async def run(args, stats: Stats = None):
    if args.command == "generate":
        await generate(args, stats)
    if args.command == "anonymize":
        await anonymize(args, stats)