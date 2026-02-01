import time
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from db import load_books_with_embeddings

from book import BookRegistry, BookTask
from settings import LIB_URL

app = FastAPI(title="Book Similarity HTML API")
registry = BookRegistry()

def make_lib_url(file_name: str) -> str:
    ex_file = file_name.removesuffix(".fb2")
    return f"{LIB_URL}/#/extended?page=1&limit=20&ex_file={ex_file}"


@app.get("/similar", response_class=HTMLResponse)
def get_similar_html(
    file: str = Query(..., description="FB2 file name"),
    limit: int = Query(50, ge=1, le=100),
    exclude_same_author: bool = Query(True)
):
    start = time.perf_counter()

    rows = load_books_with_embeddings()
    registry.bulk_add_from_db(rows)

    book_task = registry.get_book_by_name(file)
    if not book_task:
        raise HTTPException(status_code=404, detail="Book not found")

    if not book_task.embedding:
        raise HTTPException(status_code=400, detail="Book has no embedding")

    results = registry.find_similar_books(book_task, limit, exclude_same_author)

    elapsed = time.perf_counter() - start

    # HTML page
    html = f"""
    <html>
        <head>
            <title>Similar Books for {file}</title>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 80%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <h2>Top {limit} similar books for <code>{file}</code></h2>
            <p>Elapsed time: {elapsed:.3f} sec</p>
            <table>
                <tr>
                    <th>#</th>
                    <th>Score (%)</th>
                    <th>File</th>
                    <th>Title</th>
                    <th>Link</th>
                </tr>
    """

    for i, (book, score) in enumerate(results, 1):
        html += f"""
            <tr>
                <td>{i}</td>
                <td>{score * 100:.2f}</td>
                <td>{book.file_name}</td>
                <td>{book.title}</td>
                <td><a href="{make_lib_url(book.file_name)}" target="_blank">Open</a></td>
            </tr>
        """

    html += """
            </table>
        </body>
    </html>
    """

    return HTMLResponse(content=html)