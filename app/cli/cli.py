#!/usr/bin/env python3
from .args import get_args

def main():
    args = get_args()

    # Обработка команд
    if args.entity == "books":
        from app.cli.books import run
        run(args)
    if args.entity == "embedding":
        from app.cli.embedding import run
        run(args)
    elif args.entity == "similar":
        from app.cli.similar import run
        run(args)
    elif args.entity == "index":
        from app.cli.index import run
        run(args)
    elif args.entity == "feedback":
        from app.cli.feedback import run
        run(args)

if __name__ == "__main__":
    main()