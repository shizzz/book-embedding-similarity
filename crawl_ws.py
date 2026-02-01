import asyncio
import json
import sqlite3
import base64
from datetime import datetime
import websockets
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live
from rich.table import Table

# ------------------ CONFIG ------------------
BASE_URL = "https://lib.some.com"
DB_FILE = "books.db"
AUTH_HEADER = {
    "Authorization": "Basic " + base64.b64encode(b"lib:123").decode()
}

BATCH_SIZE = 20
WORKERS = 4

RATE_LIMIT = 0.05        # задержка между запросами
RETRY_COUNT = 3
RETRY_DELAY = 0.5

console = Console()

# ------------------ DB ------------------
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()

cursor.executescript("""
CREATE TABLE IF NOT EXISTS books (
    id TEXT PRIMARY KEY,
    title TEXT,
    author TEXT,
    added_at TEXT
);

CREATE TABLE IF NOT EXISTS offsets_done (
    offset INTEGER PRIMARY KEY,
    processed_at TEXT
);
""")
conn.commit()

db_lock = asyncio.Lock()

# ------------------ STATS ------------------
stats = {
    "total_found": 0,
    "offsets_done": 0,
    "books_added": 0,
    "errors": 0
}
stats_lock = asyncio.Lock()

# ------------------ UI ------------------
def make_table():
    table = Table(title="WS Library Scanner", expand=True)
    table.add_column("Metric")
    table.add_column("Value")
    for k, v in stats.items():
        table.add_row(k, str(v))
    return table

progress = Progress(
    TextColumn("[bold cyan]Overall progress"),
    BarColumn(),
    TextColumn("{task.completed}/{task.total} books"),
    TimeRemainingColumn(),
)

books_task = progress.add_task(
    "[bold green]Books processed",
    total=stats["total_found"]  # временно, уточним ниже
)

# ------------------ DB HELPERS ------------------
async def save_book(book):
    async with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO books
            (id, title, author, added_at)
            VALUES (?, ?, ?, ?)
        """, (
            book["_uid"],
            book["title"],
            book["author"],
            datetime.now().isoformat()
        ))
        conn.commit()

async def mark_offset_done(offset):
    async with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO offsets_done (offset, processed_at)
            VALUES (?, ?)
        """, (offset, datetime.now().isoformat()))
        conn.commit()

# ------------------ WS HELPERS ------------------
async def ws_request(ws, payload):
    await ws.send(json.dumps(payload))

    got_ok = False

    while True:
        msg = json.loads(await ws.recv())

        if msg.get("requestId") != payload["requestId"]:
            continue

        # статусный ответ
        if msg.get("_rok") == 1:
            got_ok = True
            continue

        # финальный ответ — только после _rok
        if got_ok:
            return msg

async def ws_request_retry(ws, payload):
    for attempt in range(RETRY_COUNT):
        try:
            await asyncio.sleep(RATE_LIMIT)
            return await ws_request(ws, payload)
        except Exception:
            if attempt + 1 == RETRY_COUNT:
                raise
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))

# ------------------ WORKER ------------------
async def worker(worker_id, offset_queue):
    request_id = 1

    async with websockets.connect(
        WS_URL,
        additional_headers=AUTH_HEADER,
        ping_interval=None
    ) as ws:
        while True:
            try:
                offset = offset_queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            # ---- search ----
            search_req = {
                "requestId": request_id,
                "action": "search",
                "from": "title",
                "query": {
                    "limit": BATCH_SIZE,
                    "offset": offset,
                    "del": "0"
                }
            }
            request_id += 1

            try:
                resp = await ws_request_retry(ws, search_req)
            except Exception:
                async with stats_lock:
                    stats["errors"] += 1
                continue

            for group in resp.get("found", []):
                for book in group.get("books", []):
                    await save_book(book)
                    progress.update(books_task, advance=1)

                    async with stats_lock:
                        stats["books_added"] += 1

            await mark_offset_done(offset)

            async with stats_lock:
                stats["offsets_done"] += 1

# ------------------ MAIN ------------------
async def main():
    # ---- test request ----
    async with websockets.connect(
        WS_URL,
        additional_headers=AUTH_HEADER,
        ping_interval=None
    ) as ws:
        test_req = {
            "requestId": 1,
            "action": "search",
            "from": "title",
            "query": {
                "limit": 1,
                "offset": 0,
                "del": "0"
            }
        }
        resp = await ws_request(ws, test_req)
        total = resp["totalFound"]
        async with stats_lock:
            stats["total_found"] = total
        progress.update(books_task, total=total)

        async with stats_lock:
            stats["total_found"] = total

    # ---- load done offsets ----
    cursor.execute("SELECT offset FROM offsets_done")
    done = {row[0] for row in cursor.fetchall()}

    # ---- load existing books ----
    cursor.execute("SELECT COUNT(*) FROM books")
    existing_books = cursor.fetchone()[0]

    # ---- учёт в статистике ----
    async with stats_lock:
        progress.update(books_task, completed=existing_books)
    async with stats_lock:
        stats["offsets_done"] = len(done)

    offset_queue = asyncio.Queue()
    for offset in range(0, total, BATCH_SIZE):
        if offset not in done:
            offset_queue.put_nowait(offset)

    # ---- workers ----
    tasks = [asyncio.create_task(worker(i + 1, offset_queue)) for i in range(WORKERS)]

    def layout():
        from rich.table import Table
        grid = Table.grid(expand=True)
        grid.add_row(make_table())
        grid.add_row(progress)
        return grid

    with Live(layout(), refresh_per_second=4, console=console) as live:
        while any(not t.done() for t in tasks):
            live.update(layout())
            await asyncio.sleep(0.5)

    await asyncio.gather(*tasks)

# ------------------ RUN ------------------
if __name__ == "__main__":
    asyncio.run(main())
