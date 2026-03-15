import argparse

from .embedding.parser import register_embedding
from .similar.parser import register_similar
from .index.parser import register_index
from .feedback.parser import register_feedback


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CLI для sim: генерация эмбеддингов, поиск похожих и др."
    )

    subparsers = parser.add_subparsers(dest="entity", required=True)

    register_embedding(subparsers)
    register_similar(subparsers)
    register_index(subparsers)
    register_feedback(subparsers)

    return parser.parse_args()