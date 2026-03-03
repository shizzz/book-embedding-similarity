from app.ui import LiveUI
from app.hnsw.trainers import LightGBMRerankerTrainer 
from app.db.router import DBRouter

def run(args):
    router = DBRouter()
    ui = LiveUI(
        max_workers = 0,
        title = "Train LightGBMReranker",
        show_table = False
    )
    ui.init()

    LightGBMRerankerTrainer(
        router=router,
        ui=ui
    ).train()