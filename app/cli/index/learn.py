from app.ui import LiveUI
from app.hnsw.trainers import LightGBMRerankerTrainer 
from app.services import TrainRerankerService
from app.infrastructure.db import DBRouter
from app.workers.stats import Stats

async def run(args, stats: Stats = None):
    router = DBRouter()
    ui = LiveUI(
        max_workers = 0,
        title = "Train LightGBMReranker",
        show_table = False,
        stats = stats
    )
    ui.init()

    TrainRerankerService(
        router, 
        ui,
        LightGBMRerankerTrainer()
    ).execute()