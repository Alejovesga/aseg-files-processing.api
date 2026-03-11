from pydantic import BaseModel
from datetime import datetime

class FileUploadResponse(BaseModel):
    file_id: str
    filename: str
    size_bytes: int
    upload_time: datetime

class FileListResponse(BaseModel):
    files: list[dict]
    total: int

class CundinamarcaProcessRequest(BaseModel):
    file_id_sisben: str
    file_id_contributivo: str
    file_id_subsidiado: str

