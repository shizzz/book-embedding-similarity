import argparse

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CLI для sim: генерация эмбеддингов, поиск похожих и др."
    )
    subparsers = parser.add_subparsers(dest="entity", required=True)

    # -----------------------
    # Команда embedding
    # -----------------------
    embedding_parser = subparsers.add_parser(
        "embedding", 
        help="Операции с эмбеддингами"
    )
    embedding_subparsers = embedding_parser.add_subparsers(dest="command", required=True)

    # embedding generate
    emb_generate_parser = embedding_subparsers.add_parser(
        "generate", 
        help="Генерация эмбеддингов"
    )
    emb_generate_parser.add_argument(
        "--batch", 
        type=str, 
        help="Количество одновременно обрабатываемых книг",
        required=True
    )

    # -----------------------
    # Команда similar
    # -----------------------
    similar_parser = subparsers.add_parser(
        "similar", 
        help="Операции с похожими объектами"
    )
    similar_subparsers = similar_parser.add_subparsers(dest="command", required=True)

    # similar generate
    sim_generate_parser = similar_subparsers.add_parser(
        "generate", 
        help="Генерация похожих объектов"
    )
    sim_generate_parser.add_argument(
        "--batch", 
        type=str, 
        help="Количество одновременно обрабатываемых книг",
        required=True
    )

    # similar get
    sim_get_parser = similar_subparsers.add_parser(
        "get", 
        help="Получение похожих"
    )
    sim_get_parser.add_argument(
        "--mode",
        choices=["index", "bruteforce"],
        required=True,
        help="Режим: index или bruteforce"
    )
    sim_get_parser.add_argument(
        "file",
        help="Путь к файлу (например file.fb2)"
    )

    # -----------------------
    # Команда index
    # -----------------------
    index_parser = subparsers.add_parser(
        "index", 
        help="Операции с HNSW индексом"
    )
    index_subparsers = index_parser.add_subparsers(dest="command", required=True)

    # index generate
    index_subparsers.add_parser(
        "generate", 
        help="Сгенерировать индекс"
    )

    # -----------------------
    # Команда feedback
    # -----------------------
    feedback_parser = subparsers.add_parser(
        "feedback", 
        help="Операции с отзывами"
    )
    feedback_subparsers = feedback_parser.add_subparsers(dest="command", required=True)
    feedback_generate_parser = feedback_subparsers.add_parser(
        "generate", 
        help="Генерация отзывов"
    )    
    feedback_generate_parser.add_argument(
        "--ai",
        choices=["chatgpt", "deepseek", "lm_studio"],
        required=True,
        help="AI api для генерации отзывов"
    )
    feedback_generate_parser.add_argument(
        "--book", 
        type=str, 
        help="Книга, для которой генерируем feedback",
        required=True
    )

    args = parser.parse_args()

    return args