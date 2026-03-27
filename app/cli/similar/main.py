import asyncio
from app.workers.stats import Stats
from .generate import run as generate
from .get_similar import run as get

def run(args, stats: Stats = None):
    if args.command == "generate":
        generate(args, stats)
    elif args.command == "get":
        asyncio.run(get(
            mode=args.mode, 
            level=args.level, 
            top=args.top, 
            file=args.file,
            stats=stats))