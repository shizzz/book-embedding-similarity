import argparse
import asyncio
import time
from typing import List, Tuple
from app.models import Similar, Embedding, Book
from app.searchEngines import SimilarSearchEngineFactory
from app.services import SimilarSearchService
from app.db import db, BookRepository, SimilarRepository, EmbeddingsRepository
from app.settings.config import LIB_URL

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"

def print_similar_books(
    similars: List[Tuple[float, int, int]],
    started_at: float
):
    elapsed = time.perf_counter() - started_at

    print(f"Время выполнения: {elapsed:.3f} сек\n")

    similars_converted = Similar.to_similar_list(similars)

    for similar in similars_converted:
        percent = similar.score * 100
        url = make_lib_url(similar.candidate.file_name)

        print(f"{percent:6.2f},{similar.candidate.file_name},{similar.candidate.title},{url}")

async def main():
    start = time.perf_counter()
    limit: int = 100
    parser = argparse.ArgumentParser(description="Показать топ-50 похожих книг")
    parser.add_argument("file_name", type=str, help="Имя файла книги")
    parser.add_argument(
        "--mode",
        choices=["bruteforce", "index"],
        default="bruteforce",
        help="Режим поиска: простой перебор (bruteforce) или HNSW-индекс (index)",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Запустить оба режима последовательно и вывести их время выполнения",
    )
    args = parser.parse_args()

    with db() as conn:
        book_task = Book.map(BookRepository().get_by_file(conn, args.file_name))

        if not book_task:
            print(f"Книга {args.file_name} не найдена в реестре")
            return
        
        embedding_bytes = EmbeddingsRepository().get(conn, book_task.id)

    if embedding_bytes is None:
        print(f"У книги {args.file_name} не сгенерирован вектор")
        return
        
    
    print(f"Поиск TOP({limit}) книг похожих на \"{book_task.title}\" {book_task.file_name}")
    embedding = Embedding.from_db(embedding_bytes)

    mode = getattr(args, "mode", SimilarSearchEngineFactory.BRUTEFORCE)
    compare = getattr(args, "compare", False)

    def run_service(selected_mode: str):
        engine = SimilarSearchEngineFactory.create(selected_mode, limit, False)
        service = SimilarSearchService(
            engine=engine,
            source=book_task,
            embedding=embedding
        )
        local_start = time.perf_counter()
        result = service.run()
        elapsed_local = time.perf_counter() - local_start
        return result, elapsed_local

    # Основной запуск в выбранном режиме
    similars, elapsed_main = run_service(mode)

    print(f"Режим '{mode}' занял {elapsed_main:.3f} сек")

    # Опциональное сравнение со вторым режимом
    if compare:
        other_mode = SimilarSearchEngineFactory.INDEX if mode == SimilarSearchEngineFactory.BRUTEFORCE else SimilarSearchEngineFactory.BRUTEFORCE
        try:
            _, elapsed_other = run_service(other_mode)
            print(f"Режим '{other_mode}' занял {elapsed_other:.3f} сек")
        except FileNotFoundError as e:
            print(f"Режим '{other_mode}' не выполнен: {e}")
    
    with db() as conn:
        SimilarRepository().replace(conn, similars)

    print_similar_books(
        similars=similars,
        started_at=start
    )

if __name__ == "__main__":
    asyncio.run(main())
