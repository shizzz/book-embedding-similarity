from argparse import _SubParsersAction, ArgumentParser
from app.infrastructure.models import SimilarSearchEngineType

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
        "--batch",
        type=int,
        help="Количество одновременно обрабатываемых книг",
        required=False
    )

def register_get(subparsers):
    parser = subparsers.add_parser(
        "get",
        help="Получение похожих"
    )

    parser.add_argument(
        "--mode",
        choices=[SimilarSearchEngineType.INDEX, SimilarSearchEngineType.BRUTEFORCE],
        required=True,
        help="Режим: index или bruteforce"
    )

    parser.add_argument(
        "file",
        help="Путь к файлу"
    )