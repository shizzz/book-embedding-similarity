from app.infrastructure.models import SimilarSearchEngineType
from app.infrastructure.models.constants import SearchIndexLevel

def register_similar(subparsers):
    parser = subparsers.add_parser(
        "similar",
        help="Операции с похожими объектами"
    )

    similar_subparsers = parser.add_subparsers(dest="command", required=True)

    register_generate(similar_subparsers)
    register_get(similar_subparsers)

def register_generate(subparsers):
    parser = subparsers.add_parser(
        "generate",
        help="Генерация похожих объектов"
    )

    parser.add_argument(
        "--level",
        choices=[SearchIndexLevel.CHUNK, SearchIndexLevel.DOCUMENT],
        required=True,
        help="Уровень поиска: CHUNK - поиск по частям, DOCUMENT - все части сливаются в один mean"
    )

    parser.add_argument(
        "-t",
        "--top",
        type=int,
        help="Количество результатов на одну книгу",
        required=False
    )

    parser.add_argument(
        "-e",
        "--exclude_same_authors",
        type=bool,
        help="Исключать из результатов книги того же автора",
        required=False
    )

def register_get(subparsers):
    parser = subparsers.add_parser(
        "get",
        help="Получение похожих"
    )

    parser.add_argument(
        "-t",
        "--top",
        type=int,
        help="Количество результатов на одну книгу",
        required=False
    )

    parser.add_argument(
        "--mode",
        choices=[SimilarSearchEngineType.INDEX, SimilarSearchEngineType.BRUTEFORCE],
        required=True,
        help="Режим: index или bruteforce"
    )

    parser.add_argument(
        "--level",
        choices=[SearchIndexLevel.CHUNK, SearchIndexLevel.DOCUMENT],
        required=True,
        help="Уровень поиска: CHUNK - поиск по частям, DOCUMENT - все части сливаются в один mean"
    )

    parser.add_argument(
        "file",
        help="Наименование файла книги"
    )