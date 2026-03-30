from app.infrastructure.models import IndexLevel

def register_index(subparsers):
    parser = subparsers.add_parser(
        "index",
        help="Операции с HNSW индексом"
    )

    index_subparsers = parser.add_subparsers(dest="command", required=True)

    register_generate(index_subparsers)
    register_learn(index_subparsers)

def register_generate(subparsers):
    parser = subparsers.add_parser(
        "generate",
        help="Сгенерировать индекс"
    )

    parser.add_argument(
        "--level",
        choices=[IndexLevel.CHUNK, IndexLevel.DOCUMENT, IndexLevel.BOTH],
        required=True,
        help="Создаваемые индексы: CHUNK, DOCUMENT, Оба",
        default=IndexLevel.BOTH,
    )

def register_learn(subparsers):
    subparsers.add_parser(
        "learn",
        help="Обучение ML модели"
    )