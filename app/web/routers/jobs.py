import uuid
import asyncio
from fastapi import APIRouter, Body
from app.web.core import run_job, jobs

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.post("/run")
async def run(data: dict = Body(...)):
    job_id = str(uuid.uuid4())

    entity = data["entity"]
    command = data["command"]
    args = data.get("args", {})
    args["command"] = command
    args["disable_ui"] = True

    asyncio.create_task(run_job(job_id, entity, args))

    return {"job_id": job_id}

@router.get("/")
async def list_jobs():
    return {
        job_id: {
            **job,
            "stats": job["stats"].to_dict() if job.get("stats") else None
        }
        for job_id, job in jobs.items()
    }

@router.get("/{job_id}")
async def get_job(job_id: str):
    stats = jobs.get(job_id)
    if not stats:
        return {"error": "Job not found"}
    return stats.to_dict()