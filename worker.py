#!/usr/bin/env python3
"""
Worker daemon that consumes video processing jobs from Kafka and processes them.
Multiple workers can run in parallel using Kafka consumer groups.
"""

import os
import sys
import json
import tempfile
import subprocess
import shutil
import re
from datetime import datetime
from urllib.parse import urlparse

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import boto3
from dotenv import load_dotenv
from db import get_db_client

load_dotenv()

# -------------------- Configuration --------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "video-processing-jobs")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "video-processors")

OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "mediaflow-1256")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "processed/")

# -------------------- Database Setup --------------------

db_service = get_db_client()

# -------------------- S3 Client --------------------

def get_s3_client():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "ap-south-2")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(
            "AWS credentials not found. Please set AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY in .env file"
        )
    
    client_kwargs = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region_name": aws_region
    }
    
    if aws_session_token:
        client_kwargs["aws_session_token"] = aws_session_token
    
    return boto3.client("s3", **client_kwargs)

# -------------------- Utility Functions --------------------

def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")

def parse_s3_url(s3_url: str) -> tuple[str, str]:
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key

def get_file_extension(path: str) -> str:
    """Extract file extension from path (local or S3)"""
    if is_s3_path(path):
        _, key = parse_s3_url(path)
        return os.path.splitext(key)[1]
    return os.path.splitext(path)[1]

def copy_to_temp(source: str) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False)
    
    if is_s3_path(source):
        s3_client = get_s3_client()
        bucket, key = parse_s3_url(source)
        s3_client.download_fileobj(bucket, key, tmp)
    else:
        with open(source, "rb") as src:
            shutil.copyfileobj(src, tmp)
    
    tmp.close()
    return tmp.name

def upload_to_s3(local_path: str, job_id: str, file_type: str) -> str:
    s3_client = get_s3_client()
    ext = os.path.splitext(local_path)[1]
    s3_key = f"{OUTPUT_PREFIX}{job_id}/{file_type}{ext}"
    
    s3_client.upload_file(local_path, OUTPUT_BUCKET, s3_key)
    return f"s3://{OUTPUT_BUCKET}/{s3_key}"

# -------------------- FFmpeg Runner with Progress --------------------

def parse_duration(duration_str: str) -> float:
    """Parse duration from format HH:MM:SS.ms to seconds"""
    try:
        parts = duration_str.split(':')
        hours = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    except:
        return 0.0

def run_ffmpeg_with_progress(command: list, job_id: str, log_file_path: str) -> tuple[bool, str]:
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True
        )

        duration = 0.0
        duration_str = None
        duration_pattern = re.compile(r"Duration: (\d{2}:\d{2}:\d{2}\.\d{2})")
        time_pattern = re.compile(r"time=(\d{2}:\d{2}:\d{2}\.\d{2})")
        last_progress = 0
        
        with open(log_file_path, 'w') as log_file:
            for line in process.stderr:
                log_file.write(line)
                log_file.flush()
                
                # Extract total duration
                if not duration:
                    duration_match = duration_pattern.search(line)
                    if duration_match:
                        duration_str = duration_match.group(1)
                        duration = parse_duration(duration_str)
                        print(f"[{job_id}] Video duration: {duration_str}")
                
                # Extract current time and calculate progress
                if duration > 0:
                    time_match = time_pattern.search(line)
                    if time_match:
                        current_time = parse_duration(time_match.group(1))
                        progress = min(int((current_time / duration) * 70) + 30, 99)
                        
                        # Update progress in database
                        db_service.update(
                            {"job_id": job_id},
                            {"progress": progress}
                        )
                        
                        # Print progress every 10% or when it changes significantly
                        if progress >= last_progress + 10:
                            print(f"[{job_id}] Progress: {progress}% ({time_match.group(1)} / {duration_str})")
                            last_progress = progress

        process.wait()

        if process.returncode == 0:
            return True, "Success"
        
        with open(log_file_path, 'r') as log_file:
            error_log = log_file.read()
        return False, error_log

    except Exception as e:
        return False, str(e)

# -------------------- Video Operations --------------------

def compress_video(source: str, output: str, job_id: str, log_path: str) -> tuple[bool, str]:
    command = [
        "ffmpeg", "-i", source,
        "-vcodec", "libx264",
        "-crf", "28",
        "-preset", "fast",
        "-progress", "pipe:2",
        "-y", output
    ]
    return run_ffmpeg_with_progress(command, job_id, log_path)

def extract_audio(source: str, output: str, job_id: str, log_path: str) -> tuple[bool, str]:
    command = [
        "ffmpeg", "-i", source,
        "-vn",
        "-acodec", "libmp3lame",
        "-progress", "pipe:2",
        "-y", output
    ]
    return run_ffmpeg_with_progress(command, job_id, log_path)

def resize_video(source: str, output: str, job_id: str, log_path: str) -> tuple[bool, str]:
    command = [
        "ffmpeg", "-i", source,
        "-vf", "scale=1280:720",
        "-progress", "pipe:2",
        "-y", output
    ]
    return run_ffmpeg_with_progress(command, job_id, log_path)

def convert_format(source: str, output: str, job_id: str, log_path: str) -> tuple[bool, str]:
    command = [
        "ffmpeg", "-i", source,
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-profile:v", "high",
        "-level", "4.2",
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-b:a", "192k",
        "-progress", "pipe:2",
        "-y", output
    ]
    return run_ffmpeg_with_progress(command, job_id, log_path)

def black_and_white(source: str, output: str, job_id: str, log_path: str) -> tuple[bool, str]:
    command = [
        "ffmpeg", "-i", source,
        "-vf", "hue=s=0",
        "-c:a", "copy",
        "-progress", "pipe:2",
        "-y", output
    ]
    return run_ffmpeg_with_progress(command, job_id, log_path)

# -------------------- Job Processing --------------------

def process_video_job(job_data: dict):
    """Process a video job from Kafka message"""
    job_id = job_data["job_id"]
    source = job_data["source"]
    method_name = job_data["method_name"]
    output_format = job_data.get("output_format")
    
    local_source = None
    output_path = None
    log_path = None
    
    try:
        print(f"[{job_id}] Starting job: {method_name} on {source}")
        db_service.update(
            {"job_id": job_id},
            {"status": "processing", "progress": 10}
        )
        print(f"[{job_id}] Status updated: processing (10%)")

        # Create temporary files for output and log
        if method_name == "extract_audio":
            suffix = ".mp3"
        elif method_name == "convert_format" and output_format:
            suffix = f".{output_format.lstrip('.')}"
        else:
            suffix = get_file_extension(source)
        
        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        output_path = temp_output.name
        temp_output.close()
        
        temp_log = tempfile.NamedTemporaryFile(delete=False, suffix=".log")
        log_path = temp_log.name
        temp_log.close()
        print(f"[{job_id}] Created temporary files: output={output_path}, log={log_path}")

        # Download source file if it's from S3
        if is_s3_path(source):
            print(f"[{job_id}] Downloading source from S3: {source}")
            local_source = copy_to_temp(source)
            print(f"[{job_id}] Downloaded to: {local_source}")
        else:
            local_source = source
            print(f"[{job_id}] Using local source: {local_source}")

        methods = {
            "compress": compress_video,
            "extract_audio": extract_audio,
            "resize": resize_video,
            "convert_format": convert_format,
            "black_and_white": black_and_white
        }

        db_service.update(
            {"job_id": job_id},
            {"progress": 30}
        )
        print(f"[{job_id}] Starting video processing: {method_name} (30%)")

        success, message = methods[method_name](local_source, output_path, job_id, log_path)

        if success:
            print(f"[{job_id}] Video processing completed successfully")
            print(f"[{job_id}] Uploading output to S3...")
            # Upload output video to S3
            s3_output_path = upload_to_s3(output_path, job_id, "output")
            print(f"[{job_id}] Output uploaded to: {s3_output_path}")
            
            print(f"[{job_id}] Uploading log to S3...")
            # Upload log file to S3
            s3_log_path = upload_to_s3(log_path, job_id, "log")
            print(f"[{job_id}] Log uploaded to: {s3_log_path}")
            
            db_service.update(
                {"job_id": job_id},
                {
                    "status": "completed",
                    "progress": 100,
                    "completed_at": datetime.utcnow().isoformat(),
                    "output_path": s3_output_path,
                    "log_path": s3_log_path
                }
            )
            print(f"[{job_id}] ✓ Job completed successfully (100%)")
            
            # Cleanup local files
            if os.path.exists(output_path):
                os.unlink(output_path)
            if os.path.exists(log_path):
                os.unlink(log_path)
            print(f"[{job_id}] Cleaned up local files")
        else:
            print(f"[{job_id}] ✗ Video processing failed: {message[:100]}...")
            # Upload failed log to S3
            s3_log_path = upload_to_s3(log_path, job_id, "log")
            
            db_service.update(
                {"job_id": job_id},
                {
                    "status": "failed",
                    "progress": 0,
                    "completed_at": datetime.utcnow().isoformat(),
                    "error": message,
                    "log_path": s3_log_path
                }
            )
            print(f"[{job_id}] Job marked as failed")
            
            if os.path.exists(output_path):
                os.unlink(output_path)
            if os.path.exists(log_path):
                os.unlink(log_path)
        
        # Clean up temporary source if it was downloaded from S3
        if local_source and is_s3_path(source) and os.path.exists(local_source):
            os.unlink(local_source)

    except Exception as e:
        # Try to upload log if it exists
        s3_log_path = None
        if log_path and os.path.exists(log_path):
            try:
                s3_log_path = upload_to_s3(log_path, job_id, "log")
            except:
                pass
        
        db_service.update(
            {"job_id": job_id},
            {
                "status": "failed",
                "progress": 0,
                "completed_at": datetime.utcnow().isoformat(),
                "error": str(e),
                "log_path": s3_log_path
            }
        )
        
        # Cleanup
        if output_path and os.path.exists(output_path):
            os.unlink(output_path)
        if log_path and os.path.exists(log_path):
            os.unlink(log_path)
        if local_source and is_s3_path(source) and os.path.exists(local_source):
            os.unlink(local_source)

# -------------------- Kafka Consumer --------------------

def main():
    """Main worker loop - consumes messages from Kafka"""
    print(f"Starting video processing worker...")
    print(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_CONSUMER_GROUP}")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    print("Worker ready. Waiting for jobs...")
    
    try:
        for message in consumer:
            try:
                job_data = message.value
                print(f"Received job: {job_data.get('job_id')}")
                process_video_job(job_data)
                print(f"Completed job: {job_data.get('job_id')}")
            except Exception as e:
                print(f"Error processing job: {e}", file=sys.stderr)
                # Continue processing other jobs
                continue
    except KeyboardInterrupt:
        print("\nShutting down worker...")
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
