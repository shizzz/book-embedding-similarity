import asyncio
from app.models import BookRegistry
from app.workers import registry, worker_startup
from app.db import load_books_with_embeddings, save_similar

registry_with_embeddings = BookRegistry()

def statBooks():
    rows = load_books_with_embeddings()
    registry.bulk_add_from_db(rows)

def getSimilar(task):
    similar = registry.find_similar_books(task, 50, False)
    save_similar(task, similar)

def main():
    asyncio.run(worker_startup(statBooks, getSimilar))

if __name__ == "__main__":
    main()
