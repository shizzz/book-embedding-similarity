# ARM-compatible Python
FROM python:3.11-slim

# ---------- system deps ----------
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# ---------- workdir ----------
WORKDIR /app

# ---------- python deps ----------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- app ----------
COPY . .

# ---------- env defaults ----------
ENV PYTHONUNBUFFERED=1 \
    LIB_URL=https://lib.ooosh.ru \
    DB_FILE=/app/data.db \
    BOOK_FOLDER=/books \
    MODEL_NAME=all-MiniLM-L6-v2 \
    MAX_WORKERS=7

# ---------- volumes ----------
VOLUME ["/books", "/app"]

# ---------- default ----------
ENTRYPOINT ["python"]
