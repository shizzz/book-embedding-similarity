from app.workers.stats import Stats
from .generate import run as generate
from .get_similar import run as get

async def run(args, stats: Stats = None):
    if args.command == "generate":
        await generate(args, stats)
    elif args.command == "get":
        await get(
            mode=args.mode, 
            level=args.level, 
            top=args.top, 
            file=args.file,
            stats=stats)