from typing import Protocol, Dict, Tuple, List
import numpy as np

class EmbeddingProvider(Protocol):
    def get_by_source_ids(
        self,
        source_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        ...

    def get_by_embedding_ids(
        self,
        embedding_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        ...