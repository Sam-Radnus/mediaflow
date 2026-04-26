import json
import os
import subprocess
import tempfile
from datetime import datetime
from urllib.parse import unquote_plus

from aws_service import AWSService


aws_service = AWSService.from_env()

DB_NAME = os.getenv("DB_NAME")  # DynamoDB table name (same as in upload_transcode.py)
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")  # If not set, falls back to source bucket
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "transcoded/")


def _update_status(job_id: str, **fields):
    """
    Helper to update DynamoDB status for a given job_id.
    """
    if not DB_NAME:
        return

    fields["updated_at"] = datetime.utcnow().isoformat()

    update_parts = [f"#{k} = :{k}" for k in fields.keys()]
    update_expression = "SET " + ", ".join(update_parts)
    expression_attribute_values = {f":{k}": v for k, v in fields.items()}
    expression_attribute_names = {f"#{k}": k for k in fields.keys()}

    aws_service.update_item_dynamodb(
        table_name=DB_NAME,
        key={"job_id": job_id},
        update_expression=update_expression,
        expression_attribute_values=expression_attribute_values,
        expression_attribute_names=expression_attribute_names,
    )


def handler(event, context):
    """
    AWS Lambda entrypoint triggered by S3 ObjectCreated events.

    For each uploaded video:
    - Marks the DynamoDB record as 'uploaded'
    - Transcodes the video to 720p using ffmpeg
    - Uploads the transcoded video back to S3
    - Updates DynamoDB status to 'completed' (or 'failed' on error)
    """
    print("Received event:", json.dumps(event))

    s3_client = aws_service.s3()

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        job_id = key  # We use the S3 key as the DynamoDB job_id

        print(f"Processing S3 object s3://{bucket}/{key}")

        try:
            # 1) Mark as uploaded
            _update_status(job_id, status="uploaded")

            # 2) Download to /tmp
            input_suffix = os.path.splitext(key)[1] or ".mp4"
            with tempfile.NamedTemporaryFile(suffix=input_suffix, delete=False) as in_tmp:
                input_path = in_tmp.name

            print(f"Downloading to {input_path}")
            s3_client.download_file(bucket, key, input_path)

            # 3) Transcode to 720p
            _update_status(job_id, status="transcoding")

            output_basename = os.path.splitext(os.path.basename(key))[0] + "_720p.mp4"
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as out_tmp:
                output_path = out_tmp.name

            print(f"Transcoding {input_path} -> {output_path}")

            # ffmpeg must be available in the Lambda runtime (e.g. via a Lambda layer)
            cmd = [
                "ffmpeg",
                "-y",
                "-i",
                input_path,
                "-vf",
                "scale=-2:720",  # Preserve aspect ratio, height=720
                "-c:v",
                "libx264",
                "-preset",
                "fast",
                "-crf",
                "23",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                output_path,
            ]

            subprocess.run(cmd, check=True)

            # 4) Upload transcoded file
            target_bucket = OUTPUT_BUCKET or bucket
            target_key = os.path.join(OUTPUT_PREFIX, output_basename)

            print(f"Uploading transcoded video to s3://{target_bucket}/{target_key}")
            s3_client.upload_file(output_path, target_bucket, target_key)

            # 5) Update status to completed
            _update_status(
                job_id,
                status="completed",
                output_bucket=target_bucket,
                output_key=target_key,
            )

            print(f"Job {job_id} completed successfully")

        except Exception as e:
            print(f"Error processing {bucket}/{key}: {e}")
            # Mark as failed in DynamoDB
            _update_status(
                job_id,
                status="failed",
                error=str(e),
            )

    return {"statusCode": 200, "body": "OK"}

