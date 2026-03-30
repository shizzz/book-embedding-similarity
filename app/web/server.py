import time
import hashlib
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.web.routers import jobs, ws, commands
from app.settings import PathsConfig

STATIC_VERSION = str(int(time.time()))
path_for_static = f"{PathsConfig.SITE_BASE_PATH}/static" if PathsConfig.SITE_BASE_PATH else "/static"
templates = Jinja2Templates(directory=f"{PathsConfig.BASE_DIR}/web/static")

app = FastAPI()
app.mount(path_for_static, StaticFiles(directory=f"{str(PathsConfig.BASE_DIR)}/web/static"), name="static")

def file_hash(path):
    data = Path(path).read_bytes()
    return hashlib.md5(data).hexdigest()[:8]

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "js_hash": file_hash(f"{PathsConfig.BASE_DIR}/web/static/app.js"),
            "css_hash": file_hash(f"{PathsConfig.BASE_DIR}/web/static/styles.css")
        }
    )

app.include_router(jobs.router)
app.include_router(ws.router)
app.include_router(commands.router)