import uuid
import asyncio
from fastapi import APIRouter
from app.web.core import run_job, jobs

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.post("/run")
async def run(entity: str, command: str, args: dict = {}):
    job_id = str(uuid.uuid4())

    asyncio.create_task(run_job(job_id, entity, command, args))

    return {"job_id": job_id}

@router.get("/")
async def list_jobs():
    return jobs

@router.get("/{job_id}")
async def get_job(job_id: str):
    return jobs.get(job_id, {"error": "not found"})