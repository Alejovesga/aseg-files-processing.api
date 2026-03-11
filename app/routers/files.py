from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from app.repositories.file_repository import FileRepository
from app.schemas.file_schema import FileUploadResponse, FileListResponse, CundinamarcaProcessRequest
from app.services.sisben_service import procesar_sisben
from app.services.maestro_service import procesar_contributivo, procesar_subsidiado
from app.services.cruces.cundinamarca_service import ejecutar_cruce
from fastapi.responses import FileResponse

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
    allowed = {".xlsx", ".csv", ".json", ".txt"}
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

@router.post("/process/cundinamarca")
async def process_cundinamarca(request: CundinamarcaProcessRequest):

    # Buscar los archivos subidos
    path_sisben       = file_repository.get_upload_path(request.file_id_sisben)
    path_contributivo = file_repository.get_upload_path(request.file_id_contributivo)
    path_subsidiado   = file_repository.get_upload_path(request.file_id_subsidiado)

    if not path_sisben:
        raise HTTPException(status_code=404, detail=f"Archivo sisben no encontrado: {request.file_id_sisben}")
    if not path_contributivo:
        raise HTTPException(status_code=404, detail=f"Archivo contributivo no encontrado: {request.file_id_contributivo}")
    if not path_subsidiado:
        raise HTTPException(status_code=404, detail=f"Archivo subsidiado no encontrado: {request.file_id_subsidiado}")

    # Procesar archivos base
    path_sisben_limpio       = file_repository.get_output_path(request.file_id_sisben)
    path_contributivo_limpio = file_repository.get_output_path(request.file_id_contributivo)
    path_subsidiado_limpio   = file_repository.get_output_path(request.file_id_subsidiado)

    procesar_sisben(path_sisben, path_sisben_limpio)
    procesar_contributivo(path_contributivo, path_contributivo_limpio)
    procesar_subsidiado(path_subsidiado, path_subsidiado_limpio)

    # Ejecutar cruce
    dir_salida = file_repository.get_output_path("cundinamarca").parent / "cundinamarca"
    zip_path = ejecutar_cruce(
        path_sisben=path_sisben_limpio,
        path_contributivo=path_contributivo_limpio,
        path_subsidiado=path_subsidiado_limpio,
        dir_salida=dir_salida,
    )

    return FileResponse(
        path=zip_path,
        filename="Cundinamarca_resultado.zip",
        media_type="application/zip"
    )