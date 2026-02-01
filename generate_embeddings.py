import os
import zipfile
import asyncio
from sentence_transformers import SentenceTransformer
from worker import registry, main
from db import load_books_only, update_book_authors, save_book_with_emb
from fb2 import FB2Book, get_file_bytes_from_zip
from settings import BOOK_FOLDER, MODEL_NAME

model = SentenceTransformer(MODEL_NAME)

def statBooks():
    completed_books = load_books_only()

    for archive in os.listdir(BOOK_FOLDER):
        if not archive.lower().endswith(".zip"):
            continue

        with zipfile.ZipFile(os.path.join(BOOK_FOLDER, archive)) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue

                completed = (archive, info.filename) in completed_books

                registry.add_book(
                    archive_name=archive,
                    file_name=info.filename,
                    completed=completed
                )

def generateEmbedding(task):
    data = get_file_bytes_from_zip(task)
    book = FB2Book(data)

    text = book.extract_text()
    id = book.get_id()
    authors = book.get_authors()
    author = ", ".join(authors)
    title = book.get_title()

    embedding = model.encode(text)

    save_book_with_emb(
        task.file_name,
        task.archive_name,
        id,
        title,
        author,
        embedding)
    
    update_book_authors(
        book=task.file_name,
        authors=authors
    )

if __name__ == "__main__":
    asyncio.run(main(statBooks, generateEmbedding))