from .generate import run as generate
from .learn import run as learn

def run(args):
    if args.command == "generate":
        generate(args)
    if args.command == "learn":
        learn(args)