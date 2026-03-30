import argparse
from .books.parser import register_books
from .embedding.parser import register_embedding
from .similar.parser import register_similar
from .index.parser import register_index
from .feedback.parser import register_feedback
from .tag.parser import register_tag

def get_args() -> argparse.Namespace:
    return build_parser().parse_args()

def build_parser():
    parser = argparse.ArgumentParser(
        description="CLI для sim: генерация эмбеддингов, поиск похожих и др."
    )

    subparsers = parser.add_subparsers(dest="entity", required=True)

    register_books(subparsers)
    register_embedding(subparsers)
    register_similar(subparsers)
    register_index(subparsers)
    register_feedback(subparsers)
    register_tag(subparsers)

    return parser