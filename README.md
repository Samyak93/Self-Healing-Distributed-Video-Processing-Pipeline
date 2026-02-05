# Self-Healing Distributed Video Processing Pipeline

This project implements a **Self-Healing Distributed Video Processing Pipeline**
for **fault-tolerant, scalable video transcoding**.
It is built using **Python**, **Celery**, **Redis**, **MongoDB**, **Docker** and **FFmpeg**.

The system follows a **coordinator–worker architecture**, where a central
orchestrator splits videos into chunks, schedules distributed transcoding jobs
across multiple resolution-specific workers, monitors worker health via
heartbeats, mitigates stragglers, and automatically recovers from failures.

---

## Project Structure

    Distributed Video Processing Pipeline/
    ├── orchestrator/
    │   ├── orchestrator.py      # Central orchestrator: chunking, scheduling, recovery, scaling
    │   ├── celery_app.py        # Celery configuration for orchestrator
    │   ├── Dockerfile           # Docker image for orchestrator service
    │   └── requirements.txt     # Python dependencies for orchestrator
    │
    ├── worker_480p/
    │   ├── worker.py            # Worker for transcoding chunks to 480p
    │   ├── celery_app.py        # Celery configuration for 480p worker
    │   ├── Dockerfile           # Docker image for 480p worker
    │   └── requirements.txt     # Python dependencies for 480p worker
    │
    ├── worker_720p/
    │   ├── worker.py            # Worker for transcoding chunks to 720p
    │   ├── celery_app.py        # Celery configuration for 720p worker
    │   ├── Dockerfile           # Docker image for 720p worker
    │   └── requirements.txt     # Python dependencies for 720p worker
    │
    ├── worker_1080p/
    │   ├── worker.py            # Worker for transcoding chunks to 1080p
    │   ├── celery_app.py        # Celery configuration for 1080p worker
    │   ├── Dockerfile           # Docker image for 1080p worker
    │   └── requirements.txt     # Python dependencies for 1080p worker
    │
    ├── videos/
    │   ├── input/               # Input videos to be processed
    │   ├── chunks/              # Intermediate video chunks
    │   └── outputs/             # Final transcoded outputs (480p/720p/1080p)
    │
    ├── docker-compose.yml       # Docker orchestration for all services
    └── README.md                # Project documentation

---

## Requirements

- Python 3.11
- Docker 24+
- MongoDB 6.0
- FFmpeg
- Message Broker (Redis via Celery)

### Python Libraries

- celery
- pymongo
- ffmpeg-python
- subprocess
- threading
  
---

## How to Run

### Start the System

    docker-compose --profile base up --build

Ensure that:
- MongoDB is reachable from all containers
- FFmpeg is available inside worker containers
- Video files are placed in the input directory:

    /videos/input

---

## How It Works

### 1. Orchestrator

The orchestrator performs the following responsibilities:

- Scans the input directory for video files
- Splits videos into fixed-duration chunks using FFmpeg
- Registers chunk metadata in MongoDB
- Dispatches chunk processing tasks via Celery
- Monitors worker liveness using heartbeat timestamps
- Detects dead workers and recovers in-flight chunks
- Mitigates stragglers using speculative execution
- Automatically scales worker containers up or down
- Merges completed chunks into final output videos
- Cleans up intermediate files safely and idempotently

---

### 2. Workers

Each worker runs inside a Docker container and is dedicated to a single resolution.

Workers:
- Periodically emit heartbeats to MongoDB
- Atomically claim pending chunks
- Transcode video chunks using FFmpeg
- Record execution metadata (start time, end time, duration)
- Safely retry on failure
- Support speculative re-execution without duplication

Worker Responsibilities by Resolution:
- worker_480.py  → 480p transcoding
- worker_720.py  → 720p transcoding
- worker_1080.py → 1080p transcoding

---

## Key Features

- Distributed Processing: Parallel chunk-level transcoding
- Fault Tolerance: Automatic recovery of failed workers
- Self-Healing: Reassigns stuck or abandoned chunks
- Straggler Mitigation: Speculative execution for slow tasks
- Auto-Scaling: Dynamically adjusts worker count based on load
- Idempotency: Safe retries without duplicate processing
- Heartbeat Monitoring: Detects failures using time-based liveness checks
- Dockerized Deployment: Fully containerized orchestration
- Persistent State: MongoDB-backed metadata and progress tracking

---

## Sample Execution Flow

- Orchestrator detects a new video file in input directory
- Video is split into N chunks
- Each chunk is queued for 480p, 720p, and 1080p processing
- Workers claim chunks and begin transcoding
- A worker failure is detected via missing heartbeat
- In-flight chunks are reset and rescheduled
- Slow chunks trigger speculative re-execution
- All chunks complete successfully
- Orchestrator merges chunks into final video files
- Intermediate artifacts are cleaned up

---

## Sample MongoDB Document (Chunk)

    [
      {
        _id: ObjectId('6984088b698a1920a516b9e0'),
        video_id: '01d7f6cb-46e7-4b6a-9aae-c73df932f5d2',
        chunk_id: 0,
        resolution: '480p',
        status: 'COMPLETED',
        worker_id: 'celery@2a5e934bf314',
        start_time: ISODate('2026-02-05T03:03:39.646Z'),
        end_time: ISODate('2026-02-05T03:03:52.722Z'),
        duration_ms: 13076,
        attempt: 1,
        speculative: false,
        output_path: '/data/videos/outputs/480p/01d7f6cb-46e7-4b6a-9aae-c73df932f5d2_chunk_000.mp4',
        created_at: ISODate('2026-02-05T03:03:39.409Z')
      }
    ]

---

## Conclusion

The Self-Healing Distributed Video Processing Pipeline provides:

- Scalable and parallel video transcoding
- Robust fault recovery and retry mechanisms
- Automated straggler detection and mitigation
- Dynamic worker scaling based on system load
- Strong consistency guarantees via atomic task claiming
- Production-grade orchestration using Docker, Celery, and MongoDB

This framework can be extended for:
- Adaptive bitrate streaming
- Cloud-based deployment (Kubernetes)
- SLA-aware scheduling
- Real-time monitoring dashboards
