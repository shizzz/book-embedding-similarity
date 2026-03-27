from app.workers.stats import Stats
from .generate import run as generate
from .learn import run as learn

def run(args, stats: Stats = None):
    if args.command == "generate":
        generate(args, stats)
    if args.command == "learn":
        learn(args, stats)