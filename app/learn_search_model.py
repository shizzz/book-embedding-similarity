import requests
from app.infrastructure.models import Feedbacks
from app.infrastructure.db.repositories import FeedbackRepository
from app.settings import PathsConfig

def sync_feedbacks(conn):
    url = f"{PathsConfig.LIB_URL}/similar/feedback/"

    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as e:
        print(f"Ошибка подключения к feedback API: {e}")
        return

    if resp.status_code != 200:
        print(f"Feedback API вернул статус {resp.status_code}, пропускаем синхронизацию")
        return

    data = resp.json().get("feedback", [])
    feedbacks = Feedbacks.from_dicts(data)
    FeedbackRepository.delete_all(conn)
    FeedbackRepository().insert_many(
        conn,
        [fb.to_db_tuple() for fb in feedbacks]
    )
