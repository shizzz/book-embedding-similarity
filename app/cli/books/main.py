from app.workers.stats import Stats
from .generate import run as generate
from .anonymize import run as anonymize

def run(args, stats: Stats = None):
    if args.command == "generate":
        generate(args, stats)
    if args.command == "anonymize":
        anonymize(args, stats)