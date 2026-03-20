from app.hnsw.trainers import RerankerTrainer
from app.hnsw.services import LTRDatasetAssembler, RerankerFeatureExtractor, RelevanceEncoder
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import FeedbackRepository, GenresRepository, CentroidsRepository
from app.infrastructure.db.services import BookEmbeddingService, PairDataLoader
from app.infrastructure.embeddings import HybridEmbeddingProvider
from app.infrastructure.books import HybridBookProvider
from app.infrastructure.models import Feedbacks, BookPair

class TrainRerankerService:
    def __init__(
        self,
        router: DBRouter,
        ui,
        trainer: RerankerTrainer
    ):
        self._feedback_repo = FeedbackRepository(router)
        self._book_provider = HybridBookProvider(router)
        self._embedding_provider = HybridEmbeddingProvider(router)
        self._embedding_service = BookEmbeddingService(router)
        self._feedbackDataLoader = PairDataLoader(self._book_provider, self._embedding_provider)
        self._ui = ui

        tag_ids = GenresRepository(router).get_ids()
        cnt_ids = CentroidsRepository(router).get_ids()

        self._dataset_builder = LTRDatasetAssembler(
            feature_extractor=RerankerFeatureExtractor(tag_ids, cnt_ids),
            label_encoder=RelevanceEncoder()
        )
        self._trainer = trainer

    def execute(self):
        feedbacks = Feedbacks(self._feedback_repo.get_all())

        if not feedbacks:
            raise ValueError("No feedbacks for training")

        books, embeddings = self._feedbackDataLoader.load(feedbacks)

        pairs = [
            BookPair.fromFeedback(fb, books, embeddings)
            for fb in feedbacks
            if BookPair.fromFeedback(fb, books, embeddings) is not None
        ]

        X, y, groups = self._dataset_builder.build(pairs)

        self._trainer.train(X, y, groups)