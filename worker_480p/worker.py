import os
import threading
import time
import subprocess
from datetime import datetime
from pymongo import MongoClient
from celery_app import celery_app

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

VIDEO_CHUNK_DIR = "/data/videos/chunks"
OUTPUT_DIR = "/data/videos/outputs/480p"

def heartbeat_loop(worker_id: str):
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    workers = db["workers"]

    while True:
        workers.update_one(
            {"worker_id": worker_id},
            {
                "$set": {
                    "worker_id": worker_id,
                    "resolution": "480p",
                    "last_heartbeat": datetime.utcnow(),
                    "status": "ALIVE",
                }
            },
            upsert=True,
        )
        time.sleep(5)

@celery_app.task(name="tasks.transcode_480p", bind=True)
def transcode_480p(self, video_id, chunk_id, chunk_path):
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    chunks = db["chunks"]

    worker_id = self.request.hostname
    
    # start heartbeat once per worker
    if not hasattr(self, "_heartbeat_started"):
        threading.Thread(
            target=heartbeat_loop,
            args=(worker_id,),
            daemon=True,
        ).start()
        self._heartbeat_started = True
        
    start_ts = datetime.utcnow()

    # ---- ATOMIC CLAIM (idempotency) ----
    doc = chunks.find_one_and_update(
        {
            "video_id": video_id,
            "chunk_id": chunk_id,
            "resolution": "480p",
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
        # Another worker already claimed or completed it
        print(f"[SKIP] Chunk {chunk_id} already claimed.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{video_id}_chunk_{chunk_id:03d}.mp4"

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", chunk_path,
            "-vf", "scale=854:480",
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
                "resolution": "480p",
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

        print(f"[DONE] 480p chunk {chunk_id}")

    except Exception as e:
        print(f"[ERROR] Chunk {chunk_id}: {e}")

        chunks.update_one(
            {
                "video_id": video_id,
                "chunk_id": chunk_id,
                "resolution": "480p",
            },
            {
                "$set": {"status": "PENDING"},
                "$unset": {
                    "worker_id": "",
                    "start_time": "",
                },
            },
        )
        raise