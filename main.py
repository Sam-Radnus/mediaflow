from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from datetime import datetime
import os
import tempfile
import uuid
import json
import boto3
from urllib.parse import urlparse
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from db import get_db_client
from schema import *

load_dotenv()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
       "*"
    ],
    allow_origin_regex=r"https://.*\.lovable\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# -------------------- Database Setup --------------------

db_service = get_db_client()
# -------------------- Kafka Configuration --------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "video-processing-jobs")

def get_kafka_producer():
    """Create and return a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

# -------------------- Models --------------------

def get_s3_client():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "ap-south-2")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(
            "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file"
        )
    
    client_kwargs = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region_name": aws_region
    }
    
    if aws_session_token:
        client_kwargs["aws_session_token"] = aws_session_token
    
    return boto3.client("s3", **client_kwargs)

def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")

def parse_s3_url(s3_url: str) -> tuple[str, str]:
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key



# -------------------- API Endpoints --------------------

@app.post("/process")
def process_video(request: VideoRequest):
    if not is_s3_path(request.source) and not os.path.exists(request.source):
        raise HTTPException(status_code=404, detail="Source file not found")

    valid_methods = {"compress", "extract_audio", "resize", "convert_format", "black_and_white"}
    if request.method_name not in valid_methods:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid method. Available: {list(valid_methods)}"
        )

    job_id = str(uuid.uuid4())

    job_doc = {
        "job_id": job_id,
        "source": request.source,
        "method_name": request.method_name,
        "output_format": request.output_format,
        "status": "queued",
        "progress": 0,
        "created_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "error": None,
        "ip": None,
        "output_path": None,
        "log_path": None
    }


    db_service.insert(job_doc)

    # Publish job to Kafka
    try:
        producer = get_kafka_producer()
        print("sending job creation message to producer")
        job_message = {
            "job_id": job_id,
            "source": request.source,
            "method_name": request.method_name,
            "output_format": request.output_format
        }
        
        future = producer.send(KAFKA_TOPIC, key=job_id, value=job_message)
        producer.flush()  # Ensure message is sent
        future.get(timeout=10)  # Wait for confirmation
        
    except KafkaError as e:
        # If Kafka fails, mark job as failed
        db_service.update(
            {"job_id": job_id},
            {
                "status": "failed",
                "error": f"Failed to queue job: {str(e)}"
            }
        )
        raise HTTPException(status_code=500, detail=f"Failed to queue job: {str(e)}")

    return {"job_id": job_id, "status": "queued"}

@app.get("/status/{job_id}", response_model=JobStatus)
def get_job_status(job_id: str):
    jobs = db_service.find({"job_id": job_id}, projection={"_id": 0}, limit=1)
    if not jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[0]

@app.get("/download/{job_id}")
def download_result(job_id: str):
    jobs = db_service.find({"job_id": job_id}, limit=1)
    if not jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[0]

    if job["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job status: {job['status']}"
        )

    output_path = job["output_path"]
    if not output_path or not is_s3_path(output_path):
        raise HTTPException(status_code=404, detail="Output file not found")

    # Download from S3 to temp file
    s3_client = get_s3_client()
    bucket, key = parse_s3_url(output_path)
    
    ext = os.path.splitext(key)[1]
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
    
    s3_client.download_file(bucket, key, temp_file.name)
    temp_file.close()

    return FileResponse(
        path=temp_file.name,
        filename=f"processed_{job_id}{ext}",
        media_type="application/octet-stream"
    )

@app.get("/process")
def list_jobs(limit: int = 10, skip: int = 0):
    jobs = db_service.find(
        {},
        sort=[("created_at", -1)],
        skip=skip,
        limit=limit
    )
    return {"jobs": jobs, "count": len(jobs)}