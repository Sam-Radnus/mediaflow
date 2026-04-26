# upload_transcode.py

import os
from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from aws_service import AWSService


load_dotenv()

aws_service = AWSService.from_env()

UPLOAD_BUCKET = os.getenv("UPLOAD_BUCKET")
DB_NAME = os.getenv("DB_NAME")  # DynamoDB table name

if not UPLOAD_BUCKET:
    raise RuntimeError("UPLOAD_BUCKET is not set in environment")
if not DB_NAME:
    raise RuntimeError("DB_NAME is not set in environment")


def uploader(file_path: str, file_name: str | None = None) -> str:
    """
    Upload a local video file to S3 and create the initial DynamoDB entry.

    - Uploads the file to the configured S3 bucket.
    - Creates a DynamoDB item with status='uploaded'.
    - Returns the S3 object key (which is also used as the job_id).
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    key = file_name or os.path.basename(file_path)

    # 1) Upload to S3 (this call blocks until the upload is complete)
    aws_service.upload_file_to_s3(
        file_path=file_path,
        bucket=UPLOAD_BUCKET,
        key=key,
    )

    # 2) Create the DynamoDB entry with status 'uploaded'
    add_entry(key)
    return key


def add_entry(s3_key: str) -> Dict:
    """
    Create the initial DynamoDB entry for an uploaded video.

    The S3 object key is used as the primary key (job_id), so the Lambda
    can easily find the record using only the S3 event (bucket + key).
    """
    item = {
        "job_id": s3_key,
        "source_bucket": UPLOAD_BUCKET,
        "source_key": s3_key,
        "status": "uploaded",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "error": None,
        "output_bucket": None,
        "output_key": None,
    }

    # This uses a plain put_item; DynamoDBService in db.py is used elsewhere
    # for job processing. Here we create the initial record.
    response = aws_service.put_item_dynamodb(DB_NAME, item)
    return response


def update_entry(job_id: str, **fields: str) -> Dict:
    """
    Utility to update an existing DynamoDB entry for a given job_id.

    Example:
        update_entry(job_id, status="transcoding")
    """
    if not fields:
        return {}

    update_parts = [f"#{k} = :{k}" for k in fields.keys()]
    update_expression = "SET " + ", ".join(update_parts)
    expression_attribute_values = {f":{k}": v for k, v in fields.items()}
    expression_attribute_names = {f"#{k}": k for k in fields.keys()}

    return aws_service.update_item_dynamodb(
        table_name=DB_NAME,
        key={"job_id": job_id},
        update_expression=update_expression,
        expression_attribute_values=expression_attribute_values,
        expression_attribute_names=expression_attribute_names,
    )


def transcoder(file_path: str, file_name: str):
    """
    Local helper (optional) to test S3 upload + DynamoDB without Lambda.

    - Uploads the file.
    - Returns the job_id (which is the S3 key).

    The actual transcoding for the production flow is handled in the
    Lambda function triggered by S3 events.
    """
    return uploader(file_path=file_path, file_name=file_name)