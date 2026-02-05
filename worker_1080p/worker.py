"""
Capstone Project: Self-Healing Distributed Video Processing Pipeline

Worker module responsible for transcoding video chunks into 1080p resolution.

Each worker:
- Registers itself via heartbeats in MongoDB
- Atomically claims video chunks from the database
- Transcodes assigned chunks using FFmpeg
- Updates execution metadata (timing, status, output paths)
- Supports failure recovery and speculative re-execution

This worker processes only 1080p video chunks.

AUTHOR: Samyak Shah CS@RIT
"""

import os
import subprocess
import threading
import time
from datetime import datetime
from pymongo import MongoClient
from celery_app import celery_app

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
OUTPUT_DIR = "/data/videos/outputs/1080p"


def heartbeat_loop(worker_id: str):
    """
    Send periodic heartbeat updates to MongoDB.

    :param worker_id: Worker identifier
    :return: None
    """
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    workers = db["workers"]

    while True:
        workers.update_one(
            {"worker_id": worker_id},
            {
                "$set": {
                    "worker_id": worker_id,
                    "container_name": os.environ["WORKER_CONTAINER_NAME"],
                    "resolution": "1080p",
                    "last_heartbeat": datetime.utcnow(),
                    "status": "ALIVE",
                }
            },
            upsert=True,
        )
        time.sleep(5)


@celery_app.task(name="tasks.transcode_1080p", bind=True)
def transcode_1080p(self, video_id, chunk_id, chunk_path):
    """
    Celery task for transcoding a video chunk into 1080p resolution.

    :param self: Celery task instance
    :param video_id: Video identifier
    :param chunk_id: Chunk index
    :param chunk_path: Path to chunk file
    :return: None
    """
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    chunks = db["chunks"]

    worker_id = self.request.hostname

    # Start heartbeat thread once per worker
    if not hasattr(self, "_heartbeat_started"):
        threading.Thread(
            target=heartbeat_loop,
            args=(worker_id,),
            daemon=True,
        ).start()
        self._heartbeat_started = True

    start_ts = datetime.utcnow()

    doc = chunks.find_one_and_update(
        {
            "video_id": video_id,
            "chunk_id": chunk_id,
            "resolution": "1080p",
            "status": "PENDING",
            # do not allow same worker to re-claim the chunk
            "$or": [
                {"worker_id": {"$ne": self.request.hostname}},
                {"worker_id": None},
            ],
        },
        {
            "$set": {
                "status": "RUNNING",
                "worker_id": self.request.hostname,
                "start_time": start_ts,
            },
            "$inc": {"attempt": 1},
        },
        return_document=True,
    )

    if doc is None:
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = f"{OUTPUT_DIR}/{video_id}_chunk_{chunk_id:03d}.mp4"

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", chunk_path,
            "-vf", "scale=1920:1080",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            output_file,
        ]

        subprocess.run(cmd, check=True)

        end_ts = datetime.utcnow()
        duration_ms = int((end_ts - start_ts).total_seconds() * 1000)

        chunks.update_one(
            {
                "video_id": video_id,
                "chunk_id": chunk_id,
                "resolution": "1080p",
                "status": "RUNNING",
            },
            {
                "$set": {
                    "status": "COMPLETED",
                    "end_time": end_ts,
                    "duration_ms": duration_ms,
                    "output_path": output_file,
                }
            },
        )

    except Exception:
        chunks.update_one(
            {
                "video_id": video_id,
                "chunk_id": chunk_id,
                "resolution": "1080p",
            },
            {
                "$set": {"status": "PENDING"},
                "$unset": {"worker_id": "", "start_time": ""},
            },
        )
        raise
