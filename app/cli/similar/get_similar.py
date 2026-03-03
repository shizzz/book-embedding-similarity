import time
import logging
from typing import List, Tuple
from app.searchEngines.similarSearch import SimilarSearchEngineFactory
from app.services import SimilarSearchService
from app.db import DBRouter
from app.db.repositories import BookRepository, SimilarRepository
from app.settings.config import LIB_URL
from app.utils import to_similar_list

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def print_similar_books(
    similars: List[Tuple[float, int, int]],
    started_at: float
):
    elapsed = time.perf_counter() - started_at

    print(f"Время выполнения: {elapsed:.3f} сек\n")

    similars_converted = to_similar_list(similars)

    for similar in similars_converted:
        percent = similar.score * 100
        url = make_lib_url(similar.candidate.file_name)

        print(f"{percent:6.2f},{similar.candidate.file_name},{similar.candidate.title},{url}")

async def run(mode: str, file: str):
    start = time.perf_counter()
    limit: int = 100
    router = DBRouter()

    book_task = BookRepository(router).get_full_by_file(file)

    if not book_task:
        print(f"Книга {file} не найдена в реестре")
        return
        
    print(f"Поиск TOP({limit}) книг похожих на \"{book_task.title}\" {book_task.file_name}")

    def run_service(mode: str):
        engine = SimilarSearchEngineFactory.create(
            mode=mode,
            router=router,
            limit=limit, 
            exclude_same_authors=True,
            logger=logger
        )
        service = SimilarSearchService(engine=engine)
        local_start = time.perf_counter()
        result = service.run(source=book_task.id)
        elapsed_local = time.perf_counter() - local_start
        return result, elapsed_local

    # Основной запуск в выбранном режиме
    similars, elapsed_main = run_service(mode)
    SimilarRepository(router).replace(similars)

    print(f"Режим '{mode}' занял {elapsed_main:.3f} сек")

    print_similar_books(
        similars=similars,
        started_at=start
    )
