import os
import zipfile
import asyncio
from sentence_transformers import SentenceTransformer
from app.workers import registry, worker_startup
from app.utils import FB2Book
from app.db import DBManager
from app.models import BookTask
from app.settings import BOOK_FOLDER, MODEL_NAME

db = DBManager()

model = SentenceTransformer(MODEL_NAME)

def statBooks():
    completed_books = db.load_books_only()

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

def generateEmbedding(task: BookTask):
    data = task.get_file_bytes_from_zip()
    book = FB2Book(data)

    text = book.extract_text()
    id = book.get_id()
    authors = book.get_authors()
    author = ", ".join(authors)
    title = book.get_title()

    embedding = model.encode(text)

    db.save_book_with_emb(
        task.file_name,
        task.archive_name,
        id,
        title,
        author,
        embedding)
    
    db.update_book_authors(
        book=task.file_name,
        authors=authors
    )

def main():
    asyncio.run(worker_startup(statBooks, generateEmbedding))

if __name__ == "__main__":
    main()
