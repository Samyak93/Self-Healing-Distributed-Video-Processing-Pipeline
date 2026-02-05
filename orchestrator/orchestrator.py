import os
import subprocess
import time
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from celery_app import celery_app

VIDEO_INPUT_DIR = "/data/videos/input"
VIDEO_CHUNK_DIR = "/data/videos/chunks"

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")

RESOLUTIONS = ["480p", "720p", "1080p"]
CHUNK_DURATION = 10  # seconds

HEARTBEAT_TIMEOUT = 30  # seconds
MONITOR_INTERVAL = 10  # seconds

# Straggler parameters
BASELINE_SAMPLE_SIZE = 100
STRAGGLER_THRESHOLD = 0.75  # 75%

# ---------- AUTO-SCALING PARAMETERS ----------
MIN_WORKERS = 1
MAX_WORKERS = 2          # keep low for localhost
SCALE_UP_THRESHOLD = 3   # pending chunks per worker
SCALE_DOWN_THRESHOLD = 1
SCALE_COOLDOWN = 60      # seconds

# Map resolution -> container name
WORKER_POOLS = {
    "480p": [
        "distributedvideoprocessingpipeline-worker_480p-1",
        "distributedvideoprocessingpipeline-worker_480p_2-1",
    ],
    "720p": [
        "distributedvideoprocessingpipeline-worker_720p-1",
        "distributedvideoprocessingpipeline-worker_720p_2-1",
    ],
    "1080p": [
        "distributedvideoprocessingpipeline-worker_1080p-1",
        "distributedvideoprocessingpipeline-worker_1080p_2-1",
    ],
}

# ---------- DESIRED WORKER STATE ----------
# resolution -> set(container_names)
DESIRED_WORKERS = {
    "480p": set([WORKER_POOLS["480p"][0]]),
    "720p": set([WORKER_POOLS["720p"][0]]),
    "1080p": set([WORKER_POOLS["1080p"][0]]),
}

# resolution -> last scale action time
LAST_SCALE_ACTION = {
    "480p": datetime.min,
    "720p": datetime.min,
    "1080p": datetime.min,
}

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


def recover_dead_workers():
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    workers_col = db["workers"]
    chunks_col = db["chunks"]

    cutoff = datetime.utcnow() - timedelta(seconds=HEARTBEAT_TIMEOUT)

    dead_workers = workers_col.find({
        "status": "ALIVE",
        "last_heartbeat": {"$lt": cutoff},
    })

    for worker in dead_workers:
        worker_id = worker["worker_id"]
        resolution = worker["resolution"]

        # Only recover workers that are supposed to be running
        container = worker.get("container_name")
        if not container or container not in DESIRED_WORKERS.get(resolution, set()):
            continue

        print(f"[DEAD] Worker {worker_id} ({resolution})")

        workers_col.update_one(
            {"worker_id": worker_id},
            {"$set": {"status": "DEAD"}}
        )

        # Recover in-flight chunks
        stuck_chunks = chunks_col.find({
            "worker_id": worker_id,
            "status": "RUNNING",
        })

        for chunk in stuck_chunks:
            print(
                f"[RECOVER] Chunk {chunk['chunk_id']} "
                f"({chunk['resolution']}) from {worker_id}"
            )

            chunks_col.update_one(
                {"_id": chunk["_id"]},
                {
                    "$set": {"status": "PENDING"},
                    "$unset": {
                        "worker_id": "",
                        "start_time": "",
                    },
                }
            )

            celery_app.send_task(
                f"tasks.transcode_{chunk['resolution']}",
                args=[
                    chunk["video_id"],
                    chunk["chunk_id"],
                    f"{VIDEO_CHUNK_DIR}/{chunk['video_id']}/chunk_{chunk['chunk_id']:03d}.mp4",
                ],
            )

        # Restart only if desired:
        docker_start(container)
        
# ---------- STRAGGLER MITIGATION ----------

def compute_latency_baselines(db):
    baselines = {}

    for res in RESOLUTIONS:
        docs = list(
            db.chunks.find(
                {
                    "resolution": res,
                    "status": "COMPLETED",
                    "duration_ms": {"$ne": None},
                }
            )
            .sort("end_time", -1)
            .limit(BASELINE_SAMPLE_SIZE)
        )

        if docs:
            baselines[res] = sum(d["duration_ms"] for d in docs) / len(docs)
    
    return baselines


def detect_and_mitigate_stragglers(db, baselines):
    now = datetime.utcnow()

    running_chunks = db.chunks.find({
        "status": "RUNNING",
        "attempt": 1,
        "speculative": {"$ne": True},
    })

    for chunk in running_chunks:
        baseline = baselines.get(chunk["resolution"])
        if not baseline or not chunk.get("start_time"):
            continue

        elapsed_ms = (now - chunk["start_time"]).total_seconds() * 1000

        if elapsed_ms > STRAGGLER_THRESHOLD * baseline:
            print(
                f"[STRAGGLER] Chunk {chunk['chunk_id']} "
                f"({chunk['resolution']}) "
                f"elapsed={int(elapsed_ms)}ms baseline={int(baseline)}ms"
            )

            updated = db.chunks.update_one(
                {
                    "_id": chunk["_id"],
                    "status": "RUNNING",
                    "speculative": {"$ne": True},
                },
                {
                    "$set": {
                        "status": "PENDING",
                        "speculative": True,
                    }
                },
            )

            if updated.modified_count == 0:
                continue

            celery_app.send_task(
                f"tasks.transcode_{chunk['resolution']}",
                args=[
                    chunk["video_id"],
                    chunk["chunk_id"],
                    f"{VIDEO_CHUNK_DIR}/{chunk['video_id']}/chunk_{chunk['chunk_id']:03d}.mp4",
                ],
            )

            print(f"[SPECULATE] Relaunched chunk {chunk['chunk_id']} ({chunk['resolution']})")


def docker_is_running(container):
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return result.stdout.strip() == "true"


def docker_start(container):
    if not docker_is_running(container):
        subprocess.run(["docker", "start", container], check=False)


def docker_stop(container):
    if docker_is_running(container):
        subprocess.run(["docker", "stop", container], check=False)
    
def auto_scale(db):
    workers_col = db["workers"]
    chunks_col = db["chunks"]
    now = datetime.utcnow()

    for res in RESOLUTIONS:
        if (now - LAST_SCALE_ACTION[res]).total_seconds() < SCALE_COOLDOWN:
            continue

        desired = DESIRED_WORKERS[res]
        pool = WORKER_POOLS[res]

        alive_workers = workers_col.count_documents({
            "resolution": res,
            "status": "ALIVE",
        })

        pending_chunks = chunks_col.count_documents({
            "resolution": res,
            "status": "PENDING",
        })

        if alive_workers == 0:
            continue
            
        alive_workers = min(alive_workers, len(desired))

        load = pending_chunks / alive_workers

        # ---- SCALE UP ----
        if load > SCALE_UP_THRESHOLD and len(desired) < MAX_WORKERS:
            for container in pool:
                if container not in desired:
                    print(f"[SCALE-UP] {res}: starting {container}")
                    desired.add(container)
                    print("DESIRED: ", desired)
                    docker_start(container)
                    LAST_SCALE_ACTION[res] = now
                    break

        # ---- SCALE DOWN ----
        elif load < SCALE_DOWN_THRESHOLD and len(desired) > MIN_WORKERS:
            container = next( c for c in reversed(WORKER_POOLS[res]) if c in desired)
            print(f"[SCALE-DOWN] {res}: stopping {container}")
            desired.remove(container)
            docker_stop(container)
            workers_col.update_many(
                {"worker_id": {"$regex": container}},
                {"$set": {"status": "STOPPED"}}
            )
            LAST_SCALE_ACTION[res] = now

def main():
    client = MongoClient(MONGO_URL)
    db = client["video_pipeline"]
    chunks_col = db["chunks"]

    # Identity (correct & safe)
    chunks_col.create_index(
        [("video_id", ASCENDING), ("chunk_id", ASCENDING), ("resolution", ASCENDING)],
        unique=True
    )

    # Performance indexes
    chunks_col.create_index([("status", ASCENDING)])
    chunks_col.create_index([("resolution", ASCENDING), ("status", ASCENDING)])
    chunks_col.create_index([("resolution", ASCENDING), ("start_time", ASCENDING)])

    if not os.path.exists(VIDEO_INPUT_DIR):
        raise RuntimeError(f"Missing input directory: {VIDEO_INPUT_DIR}")

    video_files = [
        f for f in os.listdir(VIDEO_INPUT_DIR)
        if f.lower().endswith((".mp4", ".mov", ".mkv", ".avi"))
    ]

    if video_files:
        video_file = video_files[0]
        video_path = f"{VIDEO_INPUT_DIR}/{video_file}"
        video_id = str(uuid.uuid4())

        print(f"[ORCH] Processing video: {video_file}")
        print(f"[ORCH] Video ID: {video_id}")

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
                    "speculative": False,
                    "output_path": None,
                    "created_at": datetime.utcnow(),
                }

                chunks_col.insert_one(doc)

                celery_app.send_task(
                    f"tasks.transcode_{res}",
                    args=[video_id, idx, chunk_path],
                )

                print(f"[ORCH] Queued chunk {idx} for {res}")

    print("[ORCH] Starting heartbeat monitor loop")
    
    latency_baselines = {}
    # JUST FOR TESTING IF IT ACTUALLY TRIGGERS RE-EXECUTION
    # latency_baselines = {'480p':100}
    last_baseline_refresh = datetime.utcnow()

    print("[ORCH] Starting straggler mitigation")
    
    while True:
        recover_dead_workers()
        if datetime.utcnow() - last_baseline_refresh > timedelta(minutes=1):
            latency_baselines = compute_latency_baselines(db)
            print("[ORCH] Updated latency baselines:", latency_baselines)
            last_baseline_refresh = datetime.utcnow()

        detect_and_mitigate_stragglers(db, latency_baselines)
        auto_scale(db)
        time.sleep(MONITOR_INTERVAL)


if __name__ == "__main__":
    main()