from pydantic import BaseModel
from typing import Optional

class VideoRequest(BaseModel):
    source: str
    method_name: str
    output_format: Optional[str] = None


class ThumbnailRequest(BaseModel):
    source: str
    timestamp: Optional[str] = "00:00:01"
    width: Optional[int] = None
    height: Optional[int] = None


class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: int
    created_at: str
    completed_at: Optional[str]
    error: Optional[str]
    output_path: Optional[str]
    log_path: Optional[str]
    hostname: Optional[str] = None
    