from app.ui import LiveUI
from app.hnsw.services import BookEmbeddingIndexer
from app.infrastructure.db.router import DBRouter
from app.workers.stats import NullStats

def run(args):
    router = DBRouter()
    ui = LiveUI(
        stats=NullStats(),
        max_workers = 0,
        title = "Create HNSW index",
        show_table = False
    )
    ui.init()

    BookEmbeddingIndexer(
        db_router=router,
        ui=ui
    ).build_index()