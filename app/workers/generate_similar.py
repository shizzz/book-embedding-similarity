import asyncio
from app.workers import registry, worker_startup
from app.db import DBManager
from app.models import BookTask

db = DBManager()

def statBooks():
    rows = db.load_books_with_embeddings()
    registry.bulk_add_from_db(rows)

def getSimilar(task: BookTask):
    similar = registry.find_similar_books(task, 50, False)
    db.save_similar(task, similar)

def main():
    asyncio.run(worker_startup(statBooks, getSimilar))

if __name__ == "__main__":
    main()
