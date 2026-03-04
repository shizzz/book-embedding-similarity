from app.ui import LiveUI
from app.hnsw.trainers import LightGBMRerankerTrainer 
from app.services import TrainRerankerService
from app.db import DBRouter

def run(args):
    router = DBRouter()
    ui = LiveUI(
        max_workers = 0,
        title = "Train LightGBMReranker",
        show_table = False
    )
    ui.init()

    TrainRerankerService(
        router, 
        ui,
        LightGBMRerankerTrainer()
    ).execute()