import asyncio
from worker import registry, main
from fb2 import FB2Book
from db import DBManager
from book import BookTask

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

if __name__ == "__main__":
    asyncio.run(main(statBooks, updateAuthor))