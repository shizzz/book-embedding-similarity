def register_embedding(subparsers):
    parser = subparsers.add_parser(
        "embedding",
        help="Операции с эмбеддингами"
    )

    embedding_subparsers = parser.add_subparsers(dest="command", required=True)

    register_generate(embedding_subparsers)

def register_generate(subparsers):
    parser = subparsers.add_parser(
        "generate",
        help="Генерация эмбеддингов"
    )

    parser.add_argument(
        "-b",
        "--batch",
        type=int,
        help="Количество одновременно обрабатываемых книг",
        required=False,
        default=100,
    )

    parser.add_argument(
        "-s",
        "--skip-embeddings",
        action="store_true",
        help="Пропустить генерацию самих ембеддингов",
        default=False,
    )