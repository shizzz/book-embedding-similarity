import time
import json
import asyncio
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.db import DBManager
from app.models import Book
from app.settings.config import LIB_URL, BASE_DIR
from app.services.similar_search_service import SimilarSearchService

app = FastAPI(title="Book Similarity HTML API")
app.mount("/static/css", StaticFiles(directory=f"{BASE_DIR}/static"), name="static")
templates = Jinja2Templates(directory=f"{BASE_DIR}/templates")
db = DBManager()

executor = ThreadPoolExecutor(max_workers=1)

class TaskState:
    def __init__(self):
        self.progress: int = 0
        self.result: Optional[List[Tuple[str, float, str]]] = None
        self.error: Optional[str] = None
        self.start_time: float = time.perf_counter()
        self.done_event = asyncio.Event()

    def set_progress(self, percent: int):
        self.progress = percent

    def set_done(self, rows: List[Tuple[str, float, str]]):
        self.result = rows
        self.done_event.set()

    def set_error(self, msg: str):
        self.error = msg
        self.done_event.set()

tasks: dict[str, TaskState] = {}

def update_progress(file: str, percent: int):
    if file in tasks:
        tasks[file].set_progress(percent)

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"

def render_similar_table(
    base_book: Book,
    rows: List[Tuple[str, float, str]],
    elapsed: float
) -> str:
    html = f"""
    <h2>Похожие книги для <code>{base_book.title}</code></h2>
    <p class="elapsed">Время выполнения: {elapsed:.3f} сек</p>

    <table border="1" cellpadding="6" cellspacing="0">
        <tr>
            <th>#</th>
            <th>Score (%)</th>
            <th>Файл</th>
            <th>Название</th>
            <th>Ссылка</th>
        </tr>
    """

    for i, (base_book.file_name, score, title) in enumerate(rows, 1):
        html += f"""
        <tr>
            <td>{i}</td>
            <td>{score * 100:.2f}</td>
            <td>{base_book.file_name}</td>
            <td>{title}</td>
            <td><a href="{make_lib_url(base_book.file_name)}" target="_blank">Открыть</a></td>
        </tr>
        """

    html += "</table>"
    return html

def compute_similar(book: Book, limit: int, exclude_same_author: bool):
    state = tasks[book.file_name]

    try:
        service = SimilarSearchService(
            source=book,
            limit=limit,
            exclude_same_authors=exclude_same_author,
            step_percent=1,
        )

        # Передаём callback
        top = service.run(progress_callback=lambda p: update_progress(book.file_name, p))

        # Подготавливаем строки для рендера
        rows = [(b.file_name, score, b.title) for score, b in top]

        db.save_similar(book.file_name, top)
        state.set_done(rows)

    except Exception as e:
        state.set_error(str(e))

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

# ──────────────────────────────────────────────
# SSE эндпоинт
# ──────────────────────────────────────────────
@app.get("/similar/events")
async def similar_events(
    file: str,
    limit: int = 50,
    exclude_same_author: bool = False,
    force: bool = False
):
    async def event_stream():
        start = time.perf_counter()
        book = db.get_book(file)

        if not book:         
            yield f"data: {json.dumps({'type': 'error', 'message': 'Книга не найдена'})}\n\n"

        # Кэш в БД (оставляем, т.к. это полезно)
        if not force and db.has_similar(book.file_name):
            rows = db.get_similar_rows(book.file_name, limit)
            html = render_similar_table(book, rows, time.perf_counter() - start)
            yield f"data: {json.dumps({'type': 'done', 'html': html})}\n\n"
            return

        # Получаем или создаём состояние задачи
        if book.file_name not in tasks:
            tasks[book.file_name] = TaskState()
            # Запускаем вычисление ОДИН раз — fire-and-forget
            loop = asyncio.get_running_loop()
            loop.run_in_executor(
                executor,
                compute_similar,
                book, limit, exclude_same_author
            )

        state = tasks[book.file_name]

        # Ждём прогресса / результата
        while True:
            if state.error:
                yield f"data: {json.dumps({'type': 'error', 'message': state.error})}\n\n"
                break

            if state.result is not None:
                elapsed = time.perf_counter() - start
                html = render_similar_table(book, state.result, elapsed)
                yield f"data: {json.dumps({'type': 'done', 'html': html})}\n\n"
                # Опционально: сохраняем в БД для кэша
                # db.save_similar_results(file, [(score, BookTask(...)) for ...])
                break

            # Показываем текущий прогресс
            yield f"data: {json.dumps({'type': 'progress', 'progress': state.progress})}\n\n"

            # Ждём либо события завершения, либо таймаут для polling
            try:
                await asyncio.wait_for(state.done_event.wait(), timeout=1.2)
            except asyncio.TimeoutError:
                continue

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"}
    )