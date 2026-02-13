from fastapi import APIRouter, HTTPException
from app.models import Book, FeedbackReq
from app.db import db, BookRepository, FeedbackRepository

router = APIRouter()

@router.post("/")
async def submit_feedback(fb: FeedbackReq):
    try:
        with db() as conn:
            source = Book.map(BookRepository().get_by_file(conn, fb.source_file_name))
            candidate = Book.map(BookRepository().get_by_file(conn, fb.candidate_file_name))

            FeedbackRepository().submit(conn, source.id, candidate.id, fb.label)
            
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))