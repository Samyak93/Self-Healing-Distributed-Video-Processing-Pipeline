import os
import subprocess
import uuid
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from celery_app import celery_app

VIDEO_INPUT_DIR = "/data/videos/input"
VIDEO_CHUNK_DIR = "/data/videos/chunks"

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

RESOLUTIONS = ["480p", "720p", "1080p"]
CHUNK_DURATION = 10  # seconds


def get_video_duration(video_path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_path
    ]
    return float(subprocess.check_output(cmd).decode().strip())


def split_video(video_path: str, video_id: str):
    output_dir = f"{VIDEO_CHUNK_DIR}/{video_id}"
    os.makedirs(output_dir, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-y",
        "-i", video_path,
        "-map", "0",
        "-c", "copy",
        "-f", "segment",
        "-segment_time", str(CHUNK_DURATION),
        "-force_key_frames", f"expr:gte(t,n_forced*{CHUNK_DURATION})",
        "-reset_timestamps", "1",
        f"{output_dir}/chunk_%03d.mp4"
    ]

    subprocess.run(cmd, check=True)


def main():
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    chunks_col = db["chunks"]

    # Create index once
    chunks_col.create_index(
        [("video_id", ASCENDING), ("chunk_id", ASCENDING), ("resolution", ASCENDING)],
        unique=True
    )

    if not os.path.exists(VIDEO_INPUT_DIR):
        raise RuntimeError(
            f"Input directory {VIDEO_INPUT_DIR} does not exist. "
            "Ensure it is mounted correctly and contains video files."
        )

    video_files = [
        f for f in os.listdir(VIDEO_INPUT_DIR)
        if f.lower().endswith((".mp4", ".mov", ".mkv", ".avi"))
    ]

    if not video_files:
        print(f"No video files found in {VIDEO_INPUT_DIR}. Exiting.")
        return

    video_file = video_files[0]
    video_path = f"{VIDEO_INPUT_DIR}/{video_file}"
    video_id = str(uuid.uuid4())

    print(f"Processing video: {video_file}")
    print(f"Video ID: {video_id}")

    split_video(video_path, video_id)

    chunk_files = sorted(os.listdir(f"{VIDEO_CHUNK_DIR}/{video_id}"))

    for idx, chunk_file in enumerate(chunk_files):
        chunk_path = f"{VIDEO_CHUNK_DIR}/{video_id}/{chunk_file}"

        for res in RESOLUTIONS:
            doc = {
                "video_id": video_id,
                "chunk_id": idx,
                "resolution": res,
                "status": "PENDING",
                "worker_id": None,
                "start_time": None,
                "end_time": None,
                "duration_ms": None,
                "attempt": 0,
                "output_path": None,
                "created_at": datetime.utcnow(),
            }

            chunks_col.insert_one(doc)

            task_name = f"tasks.transcode_{res}"

            celery_app.send_task(
                task_name,
                args=[video_id, idx, chunk_path],
            )

            print(f"Queued chunk {idx} for {res}")

    print("Batch 1 complete: chunks created and tasks published.")


if __name__ == "__main__":
    main()