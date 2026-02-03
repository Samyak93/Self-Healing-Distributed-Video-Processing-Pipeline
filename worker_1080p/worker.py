import os
import subprocess
from datetime import datetime
from pymongo import MongoClient
from celery_app import celery_app

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

OUTPUT_DIR = "/data/videos/outputs/1080p"


@celery_app.task(name="tasks.transcode_1080p", bind=True)
def transcode_1080p(self, video_id, chunk_id, chunk_path):
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    chunks = db["chunks"]

    worker_id = self.request.hostname
    start_ts = datetime.utcnow()

    doc = chunks.find_one_and_update(
        {
            "video_id": video_id,
            "chunk_id": chunk_id,
            "resolution": "1080p",
            "status": "PENDING",
        },
        {
            "$set": {
                "status": "RUNNING",
                "worker_id": worker_id,
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