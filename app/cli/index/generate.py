from app.ui import LiveUI
from app.hnsw.services import BookEmbeddingIndexer
from app.db.router import DBRouter

def run(args):
    router = DBRouter()
    ui = LiveUI(
        max_workers = 0,
        title = "Create HNSW index",
        show_table = False
    )
    ui.init()

    BookEmbeddingIndexer(
        db_router=router,
        ui=ui
    ).build_index()