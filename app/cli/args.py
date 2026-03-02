import argparse

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CLI для sim: генерация эмбеддингов, поиск похожих и др."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -----------------------
    # Команда generate
    # -----------------------
    generate_parser = subparsers.add_parser(
        "generate", 
        help="Генерация данных (эмбеддинги, похожие)"
    )
    generate_parser.add_argument(
        "--embedding", 
        type=str, 
        help="Параметр batch для генерации эмбеддингов"
    )
    generate_parser.add_argument(
        "--similar", 
        type=str, 
        help="Генерация похожих объектов"
    )
    generate_parser.add_argument(
        "--index", 
        action="store_true",
        help="Перестроить индекс"
    )

    # -----------------------
    # Команда get
    # -----------------------
    get_parser = subparsers.add_parser(
        "get", 
        help="Получение данных"
    )
    get_parser.add_argument(
        "--similar",
        action="store_true",
        help="Получение похожих"
    )
    get_parser.add_argument(
        "--mode",
        choices=["index", "bruteforce"],
        help="Режим: index или bruteforce"
    )
    get_parser.add_argument(
        "file",
        nargs="?",
        help="Путь к файлу (например file.fb2)"
    )

    args = parser.parse_args()
    validate_args(parser, args)

    return args

def validate_args(parser, args):
    if args.command == "get" and args.similar:
        if args.mode is None:
            parser.error("--mode обязателен при использовании --similar")

        if args.file is None:
            parser.error("file обязателен при использовании --similar")