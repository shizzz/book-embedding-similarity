from concurrent.futures import ThreadPoolExecutor
from app.db import DBRouter

executor = ThreadPoolExecutor(max_workers=1)
router = DBRouter()