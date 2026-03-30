#!/usr/bin/env python3
import asyncio
from .args import get_args

runners = {
    "books": "app.cli.books",
    "embedding": "app.cli.embedding",
    "similar": "app.cli.similar",
    "index": "app.cli.index",
    "feedback": "app.cli.feedback",
    "tag": "app.cli.tag",
}

def main():
    args = get_args()
    module_path = runners.get(args.entity)

    if not module_path:
        raise ValueError(f"Unknown entity: {args.entity}")

    module = __import__(module_path, fromlist=["run"])
    runner = module.run(args)

    if runner:
        asyncio.run(runner)

if __name__ == "__main__":
    main()