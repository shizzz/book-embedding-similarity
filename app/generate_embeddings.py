import asyncio
import argparse
from app.workers import GenerateEmbeddingsWorker
from app.settings.config import MAX_WORKERS

def main(args=None):
    batch_size = args.batch_size or 10
    db_queue_batch_size = batch_size
    queue_size = batch_size * MAX_WORKERS * 100

    worker = GenerateEmbeddingsWorker(
        title="Generate embeddings", 
        max_batch_size=batch_size,
        queue_size=queue_size,
        db_queue_batch_size=db_queue_batch_size,
        db_queue_max_size=queue_size
    )
    asyncio.run(worker.run())

def add_args(parser: argparse.ArgumentParser):
    parser.add_argument("batch_size", type=int, help="Количество книг для генерации векторов в пакете")

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Генерирование векторов книг")
    add_args(parser)
    return parser.parse_args(args)

if __name__ == "__main__":
    main(parse_args())