FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

# ---------- env defaults ----------
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    LIB_URL=https://lib.ooosh.ru \
    DB_FILE=/app/data.db \
    BOOK_FOLDER=/books \
    MODEL_NAME=all-MiniLM-L6-v2 \
    MAX_WORKERS=1

# ---------- volumes ----------
VOLUME ["/books", "/app"]

# ---------- default ----------
ENTRYPOINT ["python"]
