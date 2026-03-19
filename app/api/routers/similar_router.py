import json
import time
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from app.infrastructure.db.repositories import BookRepository, SimilarRepository, FeedbackRepository
from app.infrastructure.models import Feedbacks
from app.utils import Html
#from app.services import TaskState, Similarity
from ..dependencies import router as dbrouter#, executor
from app.settings import PathsConfig

router = APIRouter()
#similarity = Similarity(dbrouter)
path_for_static = f"{PathsConfig.SITE_BASE_PATH}/static" if PathsConfig.SITE_BASE_PATH else "/static"

@router.get("/", response_class=HTMLResponse)
async def similar_page(
    request: Request,
    file: str = Query(...),
    limit: int = Query(50, ge=1, le=100),
    exclude_same_author: bool = False,
    force: bool = False,
):
    return Html.templates.TemplateResponse(
        "similar.html",
        {
            "request": request,
            "file": file,
            "limit": limit,
            "exclude_same_author": exclude_same_author,
            "force": force,
            "path_for_static": path_for_static
        }
    )

@router.get("/events")
async def similar_events(
    request: Request,
    file: str = Query(...),
    limit: int = Query(50, ge=1, le=100),
    exclude_same_author: bool = False,
    force: bool = False,
):
    async def event_stream():
        start = time.perf_counter()

        book = BookRepository(dbrouter).get_full_by_file(file)
        if not book:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Книга не найдена'})}\n\n"
            return

        # if force:
        #     similarity.remove_task(book.file_name)

        if not force and SimilarRepository(dbrouter).has_similar(book.id):
            similars = SimilarRepository(dbrouter).get(book.id, limit)
            feedbacks = Feedbacks(FeedbackRepository(dbrouter).get(book.id))

            elapsed = time.perf_counter() - start
            html = Html.render_similar_table(request, book, similars, elapsed, feedbacks)
            yield f"data: {json.dumps({'type': 'done', 'html': html.body.decode()})}\n\n"
            return
        else:         
            yield f"data: {json.dumps({'type': 'error', 'message': 'Не найдено'})}\n\n"
            return

        # if book.file_name not in similarity.tasks:
        #     similarity.tasks[book.file_name] = TaskState(dbrouter)
        #     asyncio.get_running_loop().run_in_executor(
        #         executor,
        #         similarity.compute_similar,
        #         book, limit, exclude_same_author
        #     )

        # state = similarity.tasks[book.file_name]

        # while True:
        #     if state.error:
        #         yield f"data: {json.dumps({'type': 'error', 'message': state.error})}\n\n"
        #         break

        #     if state.result is not None:
        #         elapsed = time.perf_counter() - start            
        #         feedbacks = Feedbacks(FeedbackRepository(dbrouter).get(book.id))

        #         html = Html.render_similar_table(request, book, state.result, elapsed, feedbacks)
        #         yield f"data: {json.dumps({'type': 'done', 'html': html.body.decode()})}\n\n"
        #         break

        #     yield f"data: {json.dumps({'type': 'progress', 'progress': state.progress})}\n\n"

        #     try:
        #         await asyncio.wait_for(state.done_event.wait(), 1.2)
        #     except asyncio.TimeoutError:
        #         continue

    return StreamingResponse(event_stream(), media_type="text/event-stream")