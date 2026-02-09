import time
import json
import asyncio
import logging
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from app.db import db, SimilarRepository, BookRepository, FeedbackRepository, EmbeddingsRepository, Migrator
from app.models import Book, FeedbackReq, Similar, Embedding
from app.settings.config import LIB_URL, BASE_DIR
from app.services.similar_search_service import SimilarSearchService

app = FastAPI(title="Book Similarity HTML API")
templates = Jinja2Templates(directory=f"{BASE_DIR}/templates")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(handler)

executor = ThreadPoolExecutor(max_workers=1)

class TaskState:
    def __init__(self):
        self.progress: int = 0
        self.result: Optional[List[Similar]] = None
        self.error: Optional[str] = None
        self.start_time: float = time.perf_counter()
        self.done_event = asyncio.Event()

    def set_progress(self, percent: int):
        self.progress = percent

    def set_done(self, similars: List[Similar]):
        self.result = similars
        self.done_event.set()

    def set_error(self, msg: str):
        self.error = msg
        self.done_event.set()

tasks: dict[str, TaskState] = {}

def remove_task(file_name: str):
    if file_name in tasks:
        tasks[file_name].done_event.set()
        del tasks[file_name]

def update_progress(file: str, percent: int):
    if file in tasks:
        tasks[file].set_progress(percent)

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"

def render_similar_table(
    request: Request,
    base_book: Book,
    similars: List[Similar],
    elapsed: float
) -> HTMLResponse:
    # Подготавливаем данные для шаблона
    prepared_rows = [
        {
            "file_name": similar.candidate.file_name,
            "score": similar.score,
            "title": similar.candidate.title}
        for similar in similars
    ]

    return templates.TemplateResponse(
        "similar_table.html",
        {
            "request": request,
            "base_book": base_book,
            "rows": prepared_rows,
            "elapsed": elapsed,
            "source_file_name": base_book.file_name,
            "make_lib_url": make_lib_url,  # передаём функцию, если нужно
        }
    )

def compute_similar(book: Book, embedding: bytes, limit: int, exclude_same_author: bool):
    state = tasks[book.file_name]

    try:
        service = SimilarSearchService(
            source=book,
            embedding=Embedding.from_db(embedding),
            limit=limit,
            exclude_same_authors=exclude_same_author,
            step_percent=1,
        )

        # Передаём callback
        similars = service.run(progress_callback=lambda p: update_progress(book.file_name, p))

        with db() as conn:
            SimilarRepository().replace(conn, similars)

        state.set_done(similars)

    except Exception as e:
        state.set_error(str(e))

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("App init...")
    Migrator().apply_schema()
    logger.info("App init finished")
    yield

app.router.lifespan_context = lifespan
# =========================================================
# HTTP endpoints
# =========================================================
@app.get("/similar", response_class=HTMLResponse)
def similar_page(
    request: Request,
    file: str = Query(..., description="FB2 file name"),
    limit: int = Query(50, ge=1, le=100),
    exclude_same_author: bool = False,
    force: bool = False
):
    return templates.TemplateResponse(
        "similar.html",
        {
            "request": request,
            "file": file,
            "limit": limit,
            "exclude_same_author": exclude_same_author,
            "force": force,
        }
    )

@app.get("/similar/events")
async def similar_events(
    request: Request,
    file: str,
    limit: int = 50,
    exclude_same_author: bool = False,
    force: bool = False
):
    async def event_stream():
        start = time.perf_counter()
        with db() as conn:
            book = BookRepository().get_by_file(conn, file)

            if not book:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Книга не найдена'})}\n\n"
                return

            if force:
                remove_task(book.file_name)

            # Кэш-вариант
            if not force and SimilarRepository().has_similar(conn, book.id):
                similars = SimilarRepository().get(conn, book.id, limit)
                elapsed = time.perf_counter() - start
                response = render_similar_table(request, book, similars, elapsed)
                yield f"data: {json.dumps({'type': 'done', 'html': response.body.decode()})}\n\n"
                return
            
            embedding_raw = EmbeddingsRepository().get(conn, book.id)

        if book.file_name not in tasks:
            tasks[book.file_name] = TaskState()
            loop = asyncio.get_running_loop()
            loop.run_in_executor(
                executor,
                compute_similar,
                book, embedding_raw, limit, exclude_same_author
            )

        state = tasks[book.file_name]

        while True:
            if state.error:
                yield f"data: {json.dumps({'type': 'error', 'message': state.error})}\n\n"
                break

            if state.result is not None:
                elapsed = time.perf_counter() - start
                response = render_similar_table(request, book, state.result, elapsed)
                yield f"data: {json.dumps({'type': 'done', 'html': response.body.decode()})}\n\n"
                break

            yield f"data: {json.dumps({'type': 'progress', 'progress': state.progress})}\n\n"

            try:
                await asyncio.wait_for(state.done_event.wait(), timeout=1.2)
            except asyncio.TimeoutError:
                continue

    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.post("/feedback")
async def submit_feedback(fb: FeedbackReq):
    try:
        with db() as conn:
            source = BookRepository().get_by_file(conn, fb.source_file_name)
            candidate = BookRepository().get_by_file(conn, fb.candidate_file_name)

            FeedbackRepository.submit(conn, source.id, candidate.id)
            SimilarRepository.delete(conn, source.id)
            
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))