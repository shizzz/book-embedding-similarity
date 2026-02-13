FROM python:3.13-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app app

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    DB_FILE=/app/data/data.db \
    BOOK_FOLDER=/books \
    MODEL_NAME=all-MiniLM-L6-v2 \
    MAX_WORKERS=1

VOLUME ["/books", "/app"]

ENTRYPOINT ["python"]