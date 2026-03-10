from pydantic import BaseModel
from datetime import datetime

class FileUploadResponse(BaseModel):
    file_id: str
    filename: str
    size_bytes: int
    upload_time: datetime

class FileListResponse(BaseModel):
    files: list[FileUploadResponse]
    total: int

