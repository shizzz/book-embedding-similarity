#!/usr/bin/env python3
from .args import get_args

def main():
    args = get_args()

    # Обработка команд
    if args.command == "generate":
        from .generate import run
        run(args)
    elif args.command == "get":
        from .get.main import run
        run(args)

if __name__ == "__main__":
    main()