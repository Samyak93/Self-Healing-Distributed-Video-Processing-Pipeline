from celery import Celery
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "transcoder",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

celery_app.conf.task_routes = {
    "tasks.transcode_480p": {"queue": "transcode_480p"},
    "tasks.transcode_720p": {"queue": "transcode_720p"},
    "tasks.transcode_1080p": {"queue": "transcode_1080p"},
}