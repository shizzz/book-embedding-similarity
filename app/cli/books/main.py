from .generate import run as generate
from .anonymize import run as anonymize

def run(args):
    if args.command == "generate":
        generate(args)
    if args.command == "anonymize":
        anonymize(args)