import argparse
import time
from app.models import BookRegistry, BookTask
from app.db import DBManager
from app.settings import LIB_URL

db = DBManager()
registry = BookRegistry()

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

def main():
    start = time.perf_counter()
    parser = argparse.ArgumentParser(description="Показать топ-50 похожих книг")
    parser.add_argument("file_name", type=str, help="Имя файла книги")
    args = parser.parse_args()

    rows = db.load_books_with_embeddings()
    registry.bulk_add_from_db(rows)

    book_task = registry.get_book_by_name(args.file_name)
    if not book_task:
        print(f"Книга {args.file_name} не найдена в реестре")
        return

    if not book_task.embedding:
        print(f"Для книги {args.file_name} нет embedding")
        return
    
    similars = registry.find_similar_books(book_task, 100, False)

    print_similar_books(
        source_file=book_task.file_name,
        similars=similars,
        started_at=start
    )

if __name__ == "__main__":
    main()
