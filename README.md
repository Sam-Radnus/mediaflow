# MediaFlow

A scalable video processing service built with FastAPI, Kafka, and FFmpeg. Process videos asynchronously with support for compression, audio extraction, resizing, format conversion, and black & white filters.

## Features

- 🎬 **Multiple Video Operations**: Compress, extract audio, resize, convert format, and apply black & white filter
- 🔄 **Asynchronous Processing**: Kafka-based job queue for scalable video processing
- 📊 **Real-time Progress Tracking**: Monitor job progress in real-time
- 🗄️ **Flexible Database Support**: Switch between MongoDB and DynamoDB dynamically
- ☁️ **S3 Integration**: Automatic upload/download of source and processed files
- 🚀 **Horizontal Scaling**: Run multiple worker instances in parallel
- 📝 **Comprehensive Logging**: Detailed logs for debugging and monitoring

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   FastAPI    │─────▶│    Kafka     │─────▶│   Workers   │
│     API      │      │    Queue     │      │  (FFmpeg)   │
└─────────────┘      └──────────────┘      └─────────────┘
       │                      │                     │
       │                      │                     │
       ▼                      ▼                     ▼
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  MongoDB/   │      │  Zookeeper   │      │     S3      │
│  DynamoDB   │      │              │      │  Storage    │
└─────────────┘      └──────────────┘      └─────────────┘
```

## Prerequisites

- Python 3.9+
- FFmpeg installed on worker machines
- Docker and Docker Compose (for local development)
- AWS Account (for S3 and optional DynamoDB)
- Kafka (via Docker Compose or standalone)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Sam-Radnus/mediaflow.git
cd mediaflow
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Install FFmpeg

**macOS:**
```bash
brew install ffmpeg
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install ffmpeg
```

**Windows:**
Download from [FFmpeg official website](https://ffmpeg.org/download.html)

### 4. Start Infrastructure Services

```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka (port 9092)
- MongoDB (port 27017)

The Kafka topic `video-processing-jobs` is automatically created with 2 partitions.

## Configuration

Create a `.env` file in the project root:

```env
# Database Configuration
DB_BACKEND=mongodb                    # or "dynamodb"
DB_NAME=video_processor               # Database/Table name
MONGO_URL=mongodb://localhost:27017   # MongoDB connection string
MONGO_JOBS_COLLECTION=jobs            # MongoDB collection name

# DynamoDB Configuration (if using DynamoDB)
AWS_REGION=ap-south-2                 # AWS region
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=video-processing-jobs
KAFKA_CONSUMER_GROUP=video-processors

# S3 Configuration
OUTPUT_BUCKET=''                      # S3 bucket for processed files
OUTPUT_PREFIX=''                      # S3 key prefix
AWS_ACCESS_KEY=your_access_key        # Same as DynamoDB if using AWS
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-south-2
```

## Usage

### 1. Start the API Server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Or using Python directly:
```bash
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

### 2. Start Worker(s)

In separate terminal(s), start one or more workers:

```bash
python worker.py
```

You can run multiple workers in parallel - Kafka will automatically distribute jobs across them.

### 3. Process Videos

#### Submit a Job

```bash
curl -X POST "http://localhost:8000/process" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "s3://your-bucket/path/to/video.mp4",
    "method_name": "compress",
    "output_format": "mp4"
  }'
```

Response:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued"
}
```

#### Check Job Status

```bash
curl "http://localhost:8000/status/{job_id}"
```

Response:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "progress": 45,
  "created_at": "2024-01-15T10:30:00",
  "completed_at": null,
  "error": null,
  "output_path": null,
  "log_path": null
}
```

#### Download Processed File

```bash
curl "http://localhost:8000/download/{job_id}" --output processed_video.mp4
```

#### List Jobs

```bash
curl "http://localhost:8000/process?limit=10&skip=0"
```

## API Endpoints

### `GET /`
Health check endpoint.

**Response:**
```json
{"status": "ok"}
```

### `POST /process`
Submit a video processing job.

**Request Body:**
```json
{
  "source": "s3://bucket/path/to/video.mp4" or "/local/path/to/video.mp4",
  "method_name": "compress" | "extract_audio" | "resize" | "convert_format" | "black_and_white",
  "output_format": "mp4" (optional, for convert_format)
}
```

**Response:**
```json
{
  "job_id": "uuid",
  "status": "queued"
}
```

### `GET /status/{job_id}`
Get the status of a processing job.

**Response:**
```json
{
  "job_id": "uuid",
  "status": "queued" | "processing" | "completed" | "failed",
  "progress": 0-100,
  "created_at": "ISO timestamp",
  "completed_at": "ISO timestamp" | null,
  "error": "error message" | null,
  "output_path": "s3://bucket/path" | null,
  "log_path": "s3://bucket/path" | null
}
```

### `GET /download/{job_id}`
Download the processed video file.

**Response:** Binary file download

### `GET /process`
List all jobs (paginated).

**Query Parameters:**
- `limit` (default: 10): Number of jobs to return
- `skip` (default: 0): Number of jobs to skip

**Response:**
```json
{
  "jobs": [...],
  "count": 10
}
```

## Video Processing Methods

### `compress`
Compresses video using H.264 codec with CRF 28.

### `extract_audio`
Extracts audio track as MP3.

### `resize`
Resizes video to 1280x720 resolution.

### `convert_format`
Converts video format with optimized settings for web playback.

### `black_and_white`
Applies black and white filter to video.

## Database Support

### MongoDB (Default)

MongoDB is the default database backend. Configure using:

```env
DB_BACKEND=mongodb
DB_NAME=video_processor
MONGO_URL=mongodb://localhost:27017
MONGO_JOBS_COLLECTION=jobs
```

### DynamoDB

To use DynamoDB instead:

```env
DB_BACKEND=dynamodb
DB_NAME=video_jobs              # DynamoDB table name
AWS_REGION=ap-south-2
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

**Note:** The DynamoDB table is automatically created on first use with:
- Partition Key: `job_id` (String)
- Billing Mode: Pay-per-request

## Worker Scaling

Workers use Kafka consumer groups to automatically distribute work. To scale:

1. **Start multiple workers:**
   ```bash
   # Terminal 1
   python worker.py
   
   # Terminal 2
   python worker.py
   
   # Terminal 3
   python worker.py
   ```

2. **Kafka automatically distributes jobs** across all workers in the same consumer group.

3. **Each worker processes jobs independently** and updates progress in real-time.

## Development

### Project Structure

```
mediaflow/
├── main.py              
├── worker.py            
├── db.py                
├── schema.py            
├── docker-compose.yml  
├── requirements.txt    
└── README.md       
```

