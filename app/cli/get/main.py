import asyncio
from app.cli.get.get_similar import main

def run(args):
    if args.similar:
        asyncio.run(main(args.mode, args.file))