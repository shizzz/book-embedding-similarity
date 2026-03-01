from fastapi import APIRouter, HTTPException
from app.models import Book, FeedbackReq, Feedbacks
from app.db.repositories import BookRepository, FeedbackRepository, SimilarRepository
from ..dependencies import router as dbrouter

router = APIRouter()

@router.post("/")
async def submit_feedback(fb: FeedbackReq):
    try:
        source = Book.from_row(BookRepository(dbrouter).get_by_file(fb.source_file_name))
        candidate = Book.from_row(BookRepository(dbrouter).get_by_file(fb.candidate_file_name))

        if fb.label > 0:
            FeedbackRepository(dbrouter).submit(source.id, candidate.id, fb.label)
        elif fb.label == 0:
            FeedbackRepository(dbrouter).delete(source.id, candidate.id)
        elif fb.label < 0:
            FeedbackRepository(dbrouter).submit(source.id, candidate.id, fb.label)
            SimilarRepository(dbrouter).delete(source.id, candidate.id)

        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/")
async def get_all_feedback():
    try:
        feedbacks = Feedbacks(FeedbackRepository(dbrouter).get_all())

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