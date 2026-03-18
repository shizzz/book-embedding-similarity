def register_books(subparsers):
    parser = subparsers.add_parser(
        "books",
        help="Операции с книгами"
    )

    books_subparsers = parser.add_subparsers(dest="command", required=True)

    register_generate(books_subparsers)

def register_generate(subparsers):
    parser = subparsers.add_parser(
        "generate",
        help="Генерация данных книг"
    )

    parser.add_argument(
        "--target-chars",
        type=int,
        help="Целевое количество символов"
    )

    parser.add_argument(
        "--min-chars",
        type=int,
        help="Минимальное количество символов"
    )

    parser.add_argument(
        "--max-description-chars",
        type=int,
        help="Максимальная длина описания"
    )

    parser.add_argument(
        "--chunks",
        type=int,
        help="Целевое количество частей на книгу"
    )

    parser.add_argument(
        "--prefix-buffer",
        type=int,
        help="Размер буфера префикса"
    )

    parser.add_argument(
        "--sections-ratio",
        type=float,
        help="Регулирует деление книги на части. Если target-chars больше чем количество символов книги * sections-ratio, chunks будет занижаться"
    )