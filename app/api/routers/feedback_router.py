from fastapi import APIRouter, HTTPException
from app.models import Book, FeedbackReq, Feedbacks
from app.db import db, BookRepository, FeedbackRepository, SimilarRepository

router = APIRouter()

@router.post("/")
async def submit_feedback(fb: FeedbackReq):
    try:
        with db() as conn:
            source = Book.map(BookRepository().get_by_file(conn, fb.source_file_name))
            candidate = Book.map(BookRepository().get_by_file(conn, fb.candidate_file_name))

            if fb.label > 0:
                FeedbackRepository.submit(conn, source.id, candidate.id, fb.label)
            elif fb.label == 0:
                FeedbackRepository.delete(conn, source.id, candidate.id)
            elif fb.label < 0:
                FeedbackRepository.submit(conn, source.id, candidate.id, fb.label)
                SimilarRepository().delete(conn, source.id, candidate.id)

        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/")
async def get_all_feedback():
    try:
        with db() as conn:
            feedbacks = Feedbacks(FeedbackRepository.get_all(conn))

        return {
            "feedback": [
                {
                    "source_id": fb.source_id,
                    "candidate_id": fb.candidate_id,
                    "label": fb.label
                }
                for fb in feedbacks.items
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))