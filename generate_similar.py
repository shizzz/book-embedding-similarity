import asyncio
from worker import registry, main
from db import load_books_with_embeddings, save_similar

def statBooks():
    rows = load_books_with_embeddings()
    registry.bulk_add_from_db(rows)

def getSimilar(task):
    similar = registry.find_similar_books(task, 50, False)
    save_similar(task, similar)

# ------------------ Entry ------------------
if __name__ == "__main__":
    asyncio.run(main(statBooks, getSimilar))