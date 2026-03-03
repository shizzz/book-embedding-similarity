from typing import Protocol
from app.ui import BaseUI
from app.db import DBRouter
from app.db.repositories import BookRepository, EmbeddingsRepository, FeedbackRepository
from app.models import Feedbacks

class RerankerTrainer(Protocol):
    def __init__(
        self,
        router: DBRouter,
        ui: BaseUI,
    ):
        self._ui = ui
        self._book_repo = BookRepository(router)
        self._emp_repo = EmbeddingsRepository(router)
        self._feedbacks = Feedbacks(FeedbackRepository(router).get_all())
        
    def train(
        self, 
        top_k: int = 100
    ) -> None:
        ...
