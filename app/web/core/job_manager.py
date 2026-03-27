from .storage import jobs, subscribers

async def notify(job_id: str):
    if job_id not in subscribers:
        return

    dead = []

    for ws in subscribers[job_id]:
        try:
            await ws.send_json(jobs[job_id])
        except:
            dead.append(ws)

    # удаляем отвалившиеся сокеты
    for ws in dead:
        subscribers[job_id].remove(ws)

async def execute_cli(entity: str, command: str, args: dict, stats: dict):
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

async def run_job(job_id: str, entity: str, command: str, args: dict):
    stats = {
        "status": "running",
        "progress": 0,
        "message": "",
    }

    jobs[job_id] = stats

    await notify(job_id)

    try:
        await execute_cli(entity, command, args, stats)

        stats["status"] = "done"
    except Exception as e:
        stats["status"] = "error"
        stats["message"] = str(e)

    await notify(job_id)