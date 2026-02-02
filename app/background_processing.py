import asyncio
from app.workers import BackgroundProcessingWorker

if __name__ == "__main__":
    worker = BackgroundProcessingWorker(show_ui = False, sleepy = True)
    asyncio.run(worker.run())