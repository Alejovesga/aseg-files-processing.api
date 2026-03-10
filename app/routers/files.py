from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from app.repositories.file_repository import FileRepository
from app.schemas.file_schema import FileUploadResponse, FileListResponse

file_repository = FileRepository()

router = APIRouter(
    prefix="/files",
    tags=["files"]
)

@router.get("/", response_model=FileListResponse)
async def list_files():
    files = file_repository.list_uploads()
    return FileListResponse(
        files=files,
        total=len(files)
    )

@router.post("/upload", response_model=FileUploadResponse, status_code=201)
async def upload_file(file: UploadFile = File(...)):

    # Validar extensión
    extension = Path(file.filename).suffix.lower()
    allowed = {".xlsx", ".csv", ".json"}
    if extension not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Formato no permitido: '{extension}'"
        )

    content = await file.read()
    size_bytes = len(content)
    await file.seek(0)


    saved = file_repository.save_upload(file)

    return FileUploadResponse(
        file_id=saved["file_id"],
        filename=saved["filename"],
        size_bytes=size_bytes,
        upload_time=datetime.utcnow()
    )

@router.get("/{file_id}/download")
async def download_file(file_id: str):
    file_path = file_repository.get_upload_path(file_id)

    if file_path is None:
        raise HTTPException(
            status_code=404,
            detail=f"Archivo no encontrado: '{file_id}'"
        )

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type="application/octet-stream"
    )