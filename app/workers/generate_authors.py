import asyncio
from app.workers import registry, worker_startup
from app.utils import FB2Book
from app.db import DBManager
from app.models import BookTask

db = DBManager()

def statBooks():
    rows = db.load_books_with_authors()
    registry.bulk_add_from_db(rows)

def updateAuthor(task: BookTask):
    data = task.get_file_bytes_from_zip()
    book = FB2Book(data)
    authors = book.get_authors()
 
    db.update_book_authors(
        book=task.file_name,
        authors=authors
    )

def main():
    asyncio.run(worker_startup(statBooks, updateAuthor))

if __name__ == "__main__":
    main()
