from app.hnsw.trainers import RerankerTrainer
from app.hnsw.services import LTRDatasetAssembler, RerankerFeatureExtractor, BookPairFactory, RelevanceEncoder
from app.db import DBRouter
from app.db.repositories import BookRepository, FeedbackRepository, EmbeddingsRepository
from app.db.services import BookEmbeddingService, FeedbackDataLoader
from app.models import Feedbacks

class TrainRerankerService:
    def __init__(
        self,
        router: DBRouter,
        ui,
        trainer: RerankerTrainer
    ):
        self._feedback_repo = FeedbackRepository(router)
        self._book_repo = BookRepository(router)
        self._embedding_repo = EmbeddingsRepository(router)
        self._embedding_service = BookEmbeddingService(router)
        self._feedbackDataLoader = FeedbackDataLoader(router)
        self._ui = ui

        self._dataset_builder = LTRDatasetAssembler(
            feature_extractor=RerankerFeatureExtractor(),
            label_encoder=RelevanceEncoder()
        )
        self._trainer = trainer

    def execute(self):
        feedbacks = Feedbacks(self._feedback_repo.get_all())

        if not feedbacks:
            raise ValueError("No feedbacks for training")

        books, embeddings = self._feedbackDataLoader.load(feedbacks)

        feature_rows = []

        for fb in self._ui.tqdm(feedbacks, desc="Building dataset"):
            src = books.get(fb.source_id)
            cand = books.get(fb.candidate_id)

            src_emb = embeddings.get(fb.source_id)
            cand_emb = embeddings.get(fb.candidate_id)

            if not src or not cand:
                continue

            if src_emb is None or cand_emb is None:
                continue

            pairs = BookPairFactory().create_pairs(
                feedbacks=feedbacks,
                books=books,
                embeddings=embeddings
            )

        X, y, groups = self._dataset_builder.build(pairs)

        self._trainer.train(X, y, groups)