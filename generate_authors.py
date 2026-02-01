import asyncio
from worker import registry, main
from fb2 import FB2Book, get_file_bytes_from_zip
from db import load_books_with_authors, update_book_authors

def statBooks():
    rows = load_books_with_authors()
    registry.bulk_add_from_db(rows)

def updateAuthor(task):
    data = get_file_bytes_from_zip(task)
    book = FB2Book(data)
    authors = book.get_authors()
 
    update_book_authors(
        book=task.file_name,
        authors=authors
    )

if __name__ == "__main__":
    asyncio.run(main(statBooks, updateAuthor))