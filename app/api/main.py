import logging
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

from app.db import Migrator
from app.api.routers import similar_router, feedback_router
from app.settings.config import SITE_BASE_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(handler)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("App init...")
    Migrator().apply_schema()
    logger.info("App init finished")
    yield

app = FastAPI(title="Book Similarity HTML API", lifespan=lifespan)
app.mount(f"{SITE_BASE_PATH}/static", StaticFiles(directory="app/api/static"), name="static")

app.include_router(similar_router, prefix="/similar")
app.include_router(feedback_router, prefix="/feedback")