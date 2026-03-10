import uuid
import shutil
from datetime import datetime
from pathlib import Path
from fastapi import UploadFile

UPLOAD_DIR = Path("storage/uploads")
OUTPUT_DIR = Path("storage/outputs")

class FileRepository:

    def save_upload(self, file: UploadFile):
        file_id = str(uuid.uuid4())
        extension = Path(file.filename).suffix
        filename = f"{file_id}{extension}"
        destination = UPLOAD_DIR / filename

        with destination.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        return {
            "file_id": file_id,
            "filename": file.filename,
            "stored_as": filename,
            "path": destination
        }

    def get_output_path(self, file_id: str) -> Path:
        return OUTPUT_DIR / f"{file_id}_resultado.xlsx"

    def output_exists(self, file_id: str) -> bool:
        return self.get_output_path(file_id).exists()

    def list_uploads(self) -> list[dict]:
        files = []
        for file_path in UPLOAD_DIR.iterdir():
            if file_path.is_file():
                stat = file_path.stat()
                files.append({
                    "file_id": file_path.stem,  # nombre sin extensión = el uuid
                    "filename": file_path.name,
                    "size_bytes": stat.st_size,
                    "upload_time": datetime.fromtimestamp(stat.st_mtime)
                })
        return files

    def get_upload_path(self, file_id: str) -> Path | None:
        for file_path in UPLOAD_DIR.iterdir():
            if file_path.stem == file_id:
                return file_path
        return None