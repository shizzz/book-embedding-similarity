from typing import Type, Dict
from .chunk_strategy import ChunkStrategy
from .description_strategy import DescriptionStrategy
from .passage_strategy import PassageStrategy
from .title_strategy import TitleStrategy

class ChunkStrategyFactory:
    _strategies: Dict[int, Type[ChunkStrategy]] = {
        0: TitleStrategy,
        1: DescriptionStrategy,
        2: PassageStrategy,
    }

    @classmethod
    def create(cls, kind: int) -> ChunkStrategy:
        strategy_cls = cls._strategies.get(kind)
        if not strategy_cls:
            raise ValueError(f"No strategy found for kind '{kind}'")
        return strategy_cls()