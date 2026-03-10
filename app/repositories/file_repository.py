import uuid
import shutil
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