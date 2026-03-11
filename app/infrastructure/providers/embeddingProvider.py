from typing import Protocol, Dict, Tuple, List
import numpy as np

class EmbeddingProvider(Protocol):
    def get_by_book_ids(
        self,
        book_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        ...

    def get_by_embedding_ids(
        self,
        embedding_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        ...