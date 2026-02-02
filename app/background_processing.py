import asyncio
from app.workers import BackgroundProcessingWorker

if __name__ == "__main__":
    worker = BackgroundProcessingWorker(show_ui = False, sleepy = True, max_workers = 1)
    asyncio.run(worker.run())