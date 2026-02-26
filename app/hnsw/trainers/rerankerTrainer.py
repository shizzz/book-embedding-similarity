import numpy as np
from typing import List
from app.models import Feedbacks, Book
from typing import Protocol

class RerankerTrainer(Protocol):
    def train(
        self, 
        feedbacks: Feedbacks, 
        index, 
        books: List[Book], 
        top_k: int = 100
    ) -> None:
        ...
