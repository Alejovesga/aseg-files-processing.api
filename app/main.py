from fastapi import FastAPI
from app.routers import files
app = FastAPI(
    title="Procesador de archivos",
    description="Servicio para cruce de bases de datos",
    version="0.1.0"
)

app.include_router(files.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
