import asyncio
from fastapi import APIRouter, WebSocket
from app.web.core import subscribers, jobs

router = APIRouter(tags=["ws"])

@router.websocket("/ws/{job_id}")
async def websocket_endpoint(ws: WebSocket, job_id: str):
    await ws.accept()

    subscribers.setdefault(job_id, []).append(ws)

    try:
        while True:
            await asyncio.sleep(1)

            if job_id in jobs:
                await ws.send_json(jobs[job_id])
    finally:
        subscribers[job_id].remove(ws)