from app.workers import BaseWorker
from app.models import BookTask

class GenerateSimilarWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def stat_books(self):
        rows = self.db.load_books_with_embeddings()
        self.registry.bulk_add_from_db(rows)

    def process_book(self, task: BookTask):
        similar = self.registry.find_similar_books(task, 50, False)
        self.db.save_similar(task, similar)