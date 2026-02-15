import logging
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

from app.db import Migrator
from app.api.routers import similar_router, feedback_router
from app.settings.config import SITE_BASE_PATH, BASE_DIR

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(handler)
path_for_static = f"{SITE_BASE_PATH}/static" if SITE_BASE_PATH else "/static"

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("App init...")
    Migrator().apply_schema()
    logger.info("App init finished")
    yield

app = FastAPI(title="Book Similarity HTML API", lifespan=lifespan)
app.mount(path_for_static, StaticFiles(directory=f"{str(BASE_DIR)}/api/static"), name="static")

app.include_router(similar_router, prefix="/similar")
app.include_router(feedback_router, prefix="/similar/feedback")