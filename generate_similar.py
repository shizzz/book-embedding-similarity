import asyncio
from worker import registry, main
from db import DBManager
from book import BookTask

db = DBManager()

def statBooks():
    rows = db.load_books_with_embeddings()
    registry.bulk_add_from_db(rows)

def getSimilar(task: BookTask):
    similar = registry.find_similar_books(task, 50, False)
    db.save_similar(task, similar)

# ------------------ Entry ------------------
if __name__ == "__main__":
    asyncio.run(main(statBooks, getSimilar))