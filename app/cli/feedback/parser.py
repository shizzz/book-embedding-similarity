def register_feedback(subparsers):
    parser = subparsers.add_parser(
        "feedback",
        help="Операции с отзывами"
    )

    feedback_subparsers = parser.add_subparsers(dest="command", required=True)

    register_generate(feedback_subparsers)

def register_generate(subparsers):
    parser = subparsers.add_parser(
        "generate",
        help="Генерация отзывов",
    )

    parser.add_argument(
        "--ai",
        choices=["chatgpt", "deepseek", "lm_studio"],
        required=True,
        help="AI api для генерации отзывов",
        default="chatgpt",
    )

    parser.add_argument(
        "--book",
        type=str,
        required=True,
        help="Книга, для которой генерируем feedback",
    )