from typing import Protocol

class RerankerTrainer(Protocol):
    def train(
        self,
        feedbacks,
        embeddings,
        books
    ) -> None:
        ...
