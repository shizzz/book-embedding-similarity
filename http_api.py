import time
import json
import asyncio
from typing import List, Tuple
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from db import DBManager

from book import BookRegistry, BookTask
from settings import LIB_URL

app = FastAPI(title="Book Similarity HTML API")
db = DBManager()
registry = BookRegistry()

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"


def render_similar_table(
    base_book: str,
    rows: List[Tuple[str, float]],
    elapsed: float
) -> str:
    html = f"""
    <h2>Похожие книги для <code>{base_book}</code></h2>
    <p>Время выполнения: {elapsed:.3f} сек</p>

    <table border="1" cellpadding="6" cellspacing="0">
        <tr>
            <th>#</th>
            <th>Score (%)</th>
            <th>Файл</th>
            <th>Ссылка</th>
        </tr>
    """

    for i, (file_name, score) in enumerate(rows, 1):
        html += f"""
        <tr>
            <td>{i}</td>
            <td>{score * 100:.2f}</td>
            <td>{file_name}</td>
            <td>
                <a href="{make_lib_url(file_name)}" target="_blank">Открыть</a>
            </td>
        </tr>
        """

    html += "</table>"
    return html

def base_page(file: str, limit: int) -> str:
    return f"""
    <html>
    <head>
        <meta charset="utf-8"/>
        <title>Similar books</title>
    </head>
    <body>
        <h1>Поиск похожих книг</h1>
        <p>Книга: <code>{file}</code></p>

        <div id="status">Подготовка…</div>
        <div id="progress"></div>
        <div id="result"></div>

        <script>
            const es = new EventSource(
                "/similar/events?file={file}&limit={limit}"
            );

            es.onmessage = (e) => {{
                const data = JSON.parse(e.data);

                if (data.type === "progress") {{
                    document.getElementById("status").innerText =
                        "Запрос в процессе";
                    document.getElementById("progress").innerText =
                        "Прогресс: " + data.progress + "%";
                }}

                if (data.type === "done") {{
                    es.close();
                    document.getElementById("status").innerText = "Готово";
                    document.getElementById("progress").innerText = "";
                    document.getElementById("result").innerHTML = data.html;
                }}

                if (data.type === "error") {{
                    es.close();
                    document.getElementById("status").innerText = "Ошибка";
                    document.getElementById("result").innerText = data.message;
                }}
            }};
        </script>
    </body>
    </html>
    """

# =========================================================
# HTTP endpoints
# =========================================================

@app.get("/similar", response_class=HTMLResponse)
def similar_page(
    file: str = Query(..., description="FB2 file name"),
    limit: int = Query(50, ge=1, le=100),
):
    # базовая HTML-страница
    return HTMLResponse(base_page(file, limit))


@app.get("/similar/events")
async def similar_events(
    file: str,
    limit: int = 50,
    exclude_same_author: bool = True,
    force: bool = False
):
    async def event_stream():
        start = time.perf_counter()

        if not force and db.has_similar(file):
            rows = db.get_similar_rows(file, limit)
            html = render_similar_table(
                file, rows, time.perf_counter() - start
            )
            yield f"data: {json.dumps({'type': 'done', 'html': html})}\n\n"
            return

        db.enqueue_process(file, limit, exclude_same_author)

        # Ожидание генерации
        while True:
            in_q, progress = db.in_process_queue(file)

            if in_q:
                yield f"data: {json.dumps({'type': 'progress', 'progress': progress})}\n\n"
                await asyncio.sleep(1)
                continue

            # Очередь исчезла → результат должен быть готов
            if db.has_similar(file):
                rows = db.get_similar_rows(file, limit)
                html = render_similar_table(
                    file, rows, time.perf_counter() - start
                )
                yield f"data: {json.dumps({'type': 'done', 'html': html})}\n\n"
            else:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Ошибка обработки'})}\n\n"
            return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"}
    )