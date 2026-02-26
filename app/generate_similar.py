import asyncio
import argparse
from app.workers import GenerateSimilarWorker
from app.settings.config import MAX_WORKERS

def main(args=None):
    batch_size = args.batch_size or 10
    db_queue_batch_size = 20000
    queue_size = 0

    worker = GenerateSimilarWorker(
        max_workers = MAX_WORKERS,
        title="Generate similar", 
        batch_size=batch_size,
        queue_size=queue_size,
        db_queue_batch_size=db_queue_batch_size,
        db_queue_max_size=queue_size
    )
    asyncio.run(worker.run())

def add_args(parser: argparse.ArgumentParser):
    parser.add_argument("batch_size", type=int, help="Количество книг для поиска в пакете")

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Поиск похожих книг")
    add_args(parser)
    return parser.parse_args(args)

if __name__ == "__main__":
    main(parse_args())