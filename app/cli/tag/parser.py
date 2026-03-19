def register_tag(subparsers):
    parser = subparsers.add_parser(
        "tag",
        help="Операции с тэгами"
    )

    tag_subparsers = parser.add_subparsers(dest="command", required=True)

    register_generate(tag_subparsers)

def register_generate(subparsers):
    parser = subparsers.add_parser(
        "generate",
        help="Генерация тэгов"
    )