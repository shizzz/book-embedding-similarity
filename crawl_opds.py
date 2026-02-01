import requests
import xml.etree.ElementTree as ET
import sqlite3
import threading
import queue
from urllib.parse import urljoin, unquote
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console

# tashfile
BASE_URL = "https://lib.some.com"
USERNAME = "***"
PASSWORD = "***"
DB_FILE = "books.db"

MAX_PAGE_WORKERS = 4
TOTAL_PAGES = 3500

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "opds": "http://opds-spec.org/2010/catalog"
}

conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
db_lock = threading.Lock()

cursor.execute("""
CREATE TABLE IF NOT EXISTS books (
    book_id TEXT PRIMARY KEY,
    uid TEXT,
    title TEXT,
    author TEXT,
    link TEXT,
    added_at TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS pages_done (
    page_url TEXT PRIMARY KEY,
    processed_at TEXT
)
""")
conn.commit()

# ------------------ Очередь страниц ------------------
page_queue = queue.Queue()

# Стартовая страница поиска
with db_lock:
    cursor.execute("SELECT page_url FROM pages_done")
    done_pages = set(row[0] for row in cursor.fetchall())

# ------------------ Статистика ------------------
stats = {
    "pages_done": 0,
    "books_added": 0,
    "errors": 0,
    "active_workers": 0,
}
for i in range(1, MAX_PAGE_WORKERS + 1):
    stats[f"thread{i}"] = "-"
stats_lock = threading.Lock()

# ------------------ Rich UI ------------------
console = Console()

progress = Progress(
    TextColumn("[bold cyan]Scanning pages"),
    BarColumn(),
    TextColumn("{task.completed}/{task.total}"),
    TimeRemainingColumn(),
)

progress_task = progress.add_task("scan", total=TOTAL_PAGES)

from rich.table import Table
def make_table():
    table = Table(title="OPDS Scanner Status", expand=True)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    with stats_lock:
        table.add_row("Pages done", str(stats["pages_done"]))
        table.add_row("Books added", str(stats["books_added"]))
        table.add_row("Errors", str(stats["errors"]))
        table.add_row("Active workers", str(stats["active_workers"]))

        table.add_section()

        for i in range(1, MAX_PAGE_WORKERS + 1):
            table.add_row(f"Thread {i}", stats[f"thread{i}"])

    return table

def layout():
    grid = Table.grid(expand=True)
    grid.add_row(make_table())
    grid.add_row(progress)
    return grid

added_pages = 0
i = 0
while added_pages < MAX_PAGE_WORKERS:
    i = i + 1
    page_url = f"/opds/search?type=title&page={i}"
    if page_url not in done_pages:
        page_queue.put(page_url)
        added_pages = added_pages + 1
    else:     
        progress.update(progress_task, advance=1)

# ------------------ Функции ------------------
def fetch(url, worker_id):
    full_url = urljoin(BASE_URL, url)

    with stats_lock:
        stats[f"thread{worker_id}"] = url

    r = requests.get(full_url, auth=(USERNAME, PASSWORD), timeout=30)
    r.raise_for_status()
    return r.text

def book_exists(book_id):
    with db_lock:
        cursor.execute("SELECT 1 FROM books WHERE book_id=?", (book_id,))
        return cursor.fetchone() is not None

def add_book(book):
    with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO books (book_id, uid, title, author, link, added_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (book["book_id"], book["uid"], book["title"], book["author"], book["link"], datetime.now().isoformat()))
        conn.commit()

def mark_page_done(url):
    with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO pages_done (page_url, processed_at)
            VALUES (?, ?)
        """, (url, datetime.now().isoformat()))
        conn.commit()
        done_pages.add(url)

# ------------------ Page worker ------------------
def page_worker(worker_id, live):
    with stats_lock:
        stats["active_workers"] += 1
        stats[f"thread{worker_id}"] = "starting..."

    while True:
        try:
            url = page_queue.get(timeout=5)
        except queue.Empty:
            with stats_lock:
                stats["errors"] += 1
            break

        with db_lock:
            if url in done_pages:
                page_queue.task_done()
                continue

        try:
            xml_text = fetch(url, worker_id)
            root = ET.fromstring(xml_text)
        except Exception as e:
            print("ERROR fetching page:", url, e)
            page_queue.task_done()
            with stats_lock:
                stats["errors"] += 1
            continue

        entries = root.findall("atom:entry", NS)
        if not entries:
            # Пустая страница — больше страниц нет
            page_queue.task_done()
            continue

        books_added = 0
        subpages_to_process = []  # дочерние записи для обработки внутри этого же потока

        # --- Обработка текущей страницы ---
        for entry in entries:
            entry_id_el = entry.find("atom:id", NS)
            entry_id = entry_id_el.text if entry_id_el is not None else ""
            if entry_id.startswith("select_") or entry_id.startswith("next_page"):
                continue

            # --- Navigation / acquisition links ---
            for link in entry.findall("atom:link", NS):
                href = link.attrib.get("href", "")
                type_attr = link.attrib.get("type", "")
                kind = None
                if "kind=" in type_attr:
                    kind = type_attr.split("kind=")[-1]

                if kind == "navigation" or kind == "acquisition":
                    if "&genre=" in href:
                        href = href.replace("&genre=", "")
                    subpages_to_process.append(href)

        # --- Обрабатываем дочерние записи в рамках того же потока ---
        while subpages_to_process:
            sub_url = subpages_to_process.pop(0)
            try:
                sub_xml = fetch(sub_url, worker_id)
                sub_root = ET.fromstring(sub_xml)
            except Exception as e:
                print("ERROR fetching subpage:", sub_url, e)
                with stats_lock:
                    stats["errors"] += 1
                continue
            entries = sub_root.findall("atom:entry", NS)
            for entry in entries:
                entry_id_el = entry.find("atom:id", NS)
                entry_id = entry_id_el.text if entry_id_el is not None else ""

                if "book?uid=" in sub_url:
                    title_el = entry.find("atom:title", NS)
                    title = title_el.text if title_el is not None else ""

                    author_name = ""
                    author_el = entry.find("atom:author", NS)
                    if author_el is not None:
                        name_el = author_el.find("atom:name", NS)
                        if name_el is not None and name_el.text:
                            author_name = name_el.text.strip()

                    acq_url = None
                    for link_acq in entry.findall("atom:link", NS):
                        rel = link_acq.attrib.get("rel", "")
                        type_attr = link_acq.attrib.get("type", "")
                        if rel == "http://opds-spec.org/acquisition" and type_attr.startswith("application/fb2") and not type_attr.startswith("application/fb2+zip"):
                            acq_url = link_acq.attrib.get("href", "")
                            break

                    if acq_url and not book_exists(entry_id):
                        book_id = acq_url.split('/')[-1]
                        add_book({
                            "book_id": book_id,
                            "uid": entry_id,
                            "title": title,
                            "author": author_name,
                            "link": acq_url
                        })
                        books_added += 1

                        with stats_lock:
                            stats["books_added"] += 1
                else:
                    if entry_id.startswith("select_"):
                        continue

                    # Добавляем новые подстраницы в локальный список
                    for link in entry.findall("atom:link", NS):
                        href = link.attrib.get("href", "")
                        type_attr = link.attrib.get("type", "")
                        kind = None
                        if "kind=" in type_attr:
                            kind = type_attr.split("kind=")[-1]
                        if kind == "navigation" or kind == "acquisition":
                            if "&genre=" in href:
                                href = href.replace("&genre=", "")
                            subpages_to_process.append(href)

            with stats_lock:
                stats["pages_done"] += 1
            
            live.update(layout())

        # --- Фиксируем страницу поиска как обработанную ---
        page_num = int(url.split("page=")[-1])
        next_page = f"/opds/search?type=title&page={page_num + MAX_PAGE_WORKERS}"
        with db_lock:
            if next_page not in done_pages:
                page_queue.put(next_page)
        mark_page_done(url)

        with stats_lock:
            stats["pages_done"] += 1
        progress.update(progress_task, advance=1)
        live.update(layout())

        page_queue.task_done()

    with stats_lock:
        stats["active_workers"] -= 1
        stats[f"thread{worker_id}"] = "idle"
    live.update(layout())

# ------------------ Запуск потоков ------------------
print("Starting OPDS scan...")

# ------------------ Запуск ------------------
with Live(refresh_per_second=5, console=console) as live:
    with ThreadPoolExecutor(max_workers=MAX_PAGE_WORKERS) as executor:
        for i in range(1, MAX_PAGE_WORKERS + 1):
            executor.submit(page_worker, i, live)

    page_queue.join()

with db_lock:
    cursor.execute("SELECT COUNT(*) FROM books")
    total = cursor.fetchone()[0]

print(f"OPDS scan complete! Total books in DB: {total}")
