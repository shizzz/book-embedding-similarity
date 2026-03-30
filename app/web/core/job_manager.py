from .storage import jobs, subscribers
from app.workers.stats import PipelineStats
from typing import  Optional
from types import SimpleNamespace

async def notify(job_id: str, stats_data: Optional[dict] = None):
    """
    Отправляет обновления в WebSocket подписчикам и обновляет jobs.
    
    :param job_id: id задачи
    :param stats_data: словарь со статистикой (stats.to_dict())
    """

    # Обновляем глобальное состояние
    if job_id not in jobs:
        jobs[job_id] = {}

    if stats_data:
        jobs[job_id]["stats"] = stats_data

    # Проверяем статус, если его нет – ставим running
    if "status" not in jobs[job_id]:
        jobs[job_id]["status"] = "running"

    # Отправляем всем подписчикам WebSocket
    if job_id not in subscribers:
        return

    dead_connections = []

    for ws in subscribers[job_id]:
        try:
            await ws.send_json(jobs[job_id])
        except Exception:
            # соединение потеряно, удалим позже
            dead_connections.append(ws)

    # Удаляем “мертвые” соединения
    for ws in dead_connections:
        subscribers[job_id].remove(ws)

async def execute_cli(entity: str, data: dict, stats: dict):
    args = SimpleNamespace(**data)
    if entity == "books":
        from app.cli.books import run
        await run(args, stats)

    elif entity == "embedding":
        from app.cli.embedding import run
        await run(args, stats)

    elif entity == "tag":
        from app.cli.tag import run
        await run(args, stats)

    elif entity == "index":
        from app.cli.index import run
        await run(args, stats)

    elif entity == "similar":
        from app.cli.similar import run
        await run(args, stats)

    elif entity == "feedback":
        from app.cli.feedback import run
        await run(args, stats)

async def run_job(job_id, entity, args):
    stats = PipelineStats(job_id=job_id, notifier=notify)
    jobs[job_id] = {"status": "running", "stats": stats}

    try:
        await execute_cli(entity, args, stats)

        jobs[job_id]["status"] = "done"
        jobs[job_id]["message"] = ""
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = str(e)

    await notify(job_id)