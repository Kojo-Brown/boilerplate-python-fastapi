"""Celery application factory.

Start the worker:
    celery -A src.worker.celery_app worker --loglevel=info

Start with concurrency and queues:
    celery -A src.worker.celery_app worker -c 4 -Q default,email --loglevel=info

Inspect running workers:
    celery -A src.worker.celery_app inspect active
"""

from celery import Celery

from src.config import settings

celery_app = Celery(
    "boilerplate",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["src.tasks.celery_email"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=3600,
    task_routes={
        "tasks.send_welcome_email": {"queue": "email"},
        "tasks.send_password_reset_email": {"queue": "email"},
    },
)
