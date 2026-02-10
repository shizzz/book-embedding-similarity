from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from app.models import Book, FeedbackReq
from app.db import db, BookRepository, FeedbackRepository, SimilarRepository

router = APIRouter()

@router.post("/", response_class=HTMLResponse)
async def submit_feedback(fb: FeedbackReq):
    try:
        with db() as conn:
            source = Book.map(BookRepository().get_by_file(conn, fb.source_file_name))
            candidate = Book.map(BookRepository().get_by_file(conn, fb.candidate_file_name))

            FeedbackRepository().submit(conn, source.id, candidate.id, fb.label)
            SimilarRepository().delete(conn, source.id)
            
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))