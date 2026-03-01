#!/usr/bin/env python3
import asyncio
import argparse
from app.settings.config import MAX_WORKERS

def main():
    parser = argparse.ArgumentParser(
        description="CLI для sim: генерация эмбеддингов, поиск похожих и др."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -----------------------
    # Команда generate
    # -----------------------
    generate_parser = subparsers.add_parser(
        "generate", help="Генерация данных (эмбеддинги, похожие)"
    )
    generate_parser.add_argument(
        "--embedding", type=str, help="Параметр batch для генерации эмбеддингов"
    )
    generate_parser.add_argument(
        "--similar", action="store_true", help="Генерация похожих объектов"
    )

    args = parser.parse_args()

    # Обработка команд
    if args.command == "generate":
        if args.embedding:
            from app.workers.generate_embeddings import GenerateEmbeddingsWorker
            
            batch_value = None
            if "=" in args.embedding:
                key, val = args.embedding.split("=", 1)
                if key == "batch":
                    batch_value = int(val)

            batch_size = batch_value or 10
            db_queue_batch_size = batch_size
            queue_size = batch_size * MAX_WORKERS * 3

            worker = GenerateEmbeddingsWorker(
                title="Generate embeddings", 
                max_batch_size=batch_size,
                queue_size=queue_size,
                db_queue_batch_size=db_queue_batch_size,
                db_queue_max_size=queue_size
            )
            asyncio.run(worker.run())

if __name__ == "__main__":
    main()