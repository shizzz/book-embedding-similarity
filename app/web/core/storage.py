from typing import Dict, List
from fastapi import WebSocket

# Все задачи
jobs: Dict[str, dict] = {}

# Подписчики по job_id
subscribers: Dict[str, List[WebSocket]] = {}