import sqlite3
import pickle
import numpy as np
from flask import Flask, request, abort

DB_FILE = "books.db"

app = Flask(__name__)

# ---------- Вспомогательные функции ----------

def cosine_similarity(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn

def load_embedding(conn, book_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT embedding FROM embeddings WHERE book_id = ?",
        (book_id,)
    )
    row = cur.fetchone()
    if not row:
        return None
    return pickle.loads(row[0])

# ---------- HTTP обработчик ----------

@app.route("/similar")
def similar_books():
    uid = request.args.get("uid")
    limit = int(request.args.get("limit", 10))

    if not uid:
        abort(400, "uid parameter required")

    conn = get_db()
    cur = conn.cursor()

    base_emb = load_embedding(conn, uid)
    if base_emb is None:
        abort(404, "Embedding not found for this book")

    # Получаем все embeddings кроме текущей книги
    cur.execute("""
        SELECT b.id, b.title, b.author, b.link, e.embedding
        FROM embeddings e
        JOIN books b ON b.id = e.book_id
        WHERE b.id != ?
    """, (id,))

    results = []

    for id, title, author, emb_blob in cur.fetchall():
        emb = pickle.loads(emb_blob)
        score = cosine_similarity(base_emb, emb)
        results.append((score, title, author))

    # Сортировка по убыванию похожести
    results.sort(reverse=True, key=lambda x: x[0])
    results = results[:limit]

    # ---------- HTML ----------
    html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Похожие книги</title>
    </head>
    <body>
        <h2>Похожие книги</h2>
        <ol>
    """

    for score, title, author in results:
        percent = round(score * 100, 2)
        html += f"""
        <li>
            <b>{percent}%</b> — {title}<br>
            <i>{author}</i><br>
        </li>
        <br>
        """

    html += """
        </ol>
    </body>
    </html>
    """

    return html

# ---------- Запуск ----------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
    