import argparse
import asyncio
import time
from app.models import BookRegistry, BookTask, TaskRegistry
from app.services.similar_search_service import SimilarSearchService
from app.db import DBManager
from app.settings.config import LIB_URL

db = DBManager()

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"

def print_similar_books(
    source_file: str,
    similars: list[tuple[BookTask, float]],
    started_at: float
):
    elapsed = time.perf_counter() - started_at

    print(f"\nТоп-50 похожих книг для {source_file}")
    print(f"Время выполнения: {elapsed:.3f} сек\n")

    for book, score in similars:
        percent = score * 100
        url = make_lib_url(book.file_name)

        print(f"{percent:6.2f},{book.file_name},{book.title},{url}")

async def main():
    books = BookRegistry()

    start = time.perf_counter()
    parser = argparse.ArgumentParser(description="Показать топ-50 похожих книг")
    parser.add_argument("file_name", type=str, help="Имя файла книги")
    args = parser.parse_args()

    rows = await db.load_books_with_embeddings()
    books.add_books(rows)

    book_task = books.get_book_by_name(args.file_name)
    if not book_task:
        print(f"Книга {args.file_name} не найдена в реестре")
        return

    if not book_task.embedding:
        print(f"Для книги {args.file_name} нет embedding")
        return

    service = SimilarSearchService(
        source=book_task,
        registry=books,
        limit=100,
        exclude_same_authors=False,
        step_percent=5)
    
    similars = service.run()
    db.save_similar(book_task.file_name, similars)

    print_similar_books(
        source_file=book_task.file_name,
        similars=similars,
        started_at=start
    )

if __name__ == "__main__":
    asyncio.run(main())
