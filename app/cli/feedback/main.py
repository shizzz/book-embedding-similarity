from .generate import run as generate

def run(args):
    if args.command == "generate":
        generate(args)