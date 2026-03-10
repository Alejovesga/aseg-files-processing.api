from fastapi import APIRouter
from app.schemas.file_schema import  FileListResponse

router = APIRouter(
    prefix="/files",
    tags=["files"]
)

@router.get("/", response_model=FileListResponse)
async def list_files():
    return {"files": [], "total": 0}