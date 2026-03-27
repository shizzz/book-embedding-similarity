from app.workers.stats import Stats
from .generate import run as generate

def run(args, stats: Stats = None):
    if args.command == "generate":
        generate(args, stats)