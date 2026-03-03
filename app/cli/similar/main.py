import asyncio
from .generate import run as generate
from .get_similar import run as get

def run(args):
    if args.command == "generate":
        generate(args)
    elif args.command == "get":
        asyncio.run(get(args.mode, args.file))