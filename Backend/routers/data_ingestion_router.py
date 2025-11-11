# routers/ingestion_router.py
from fastapi import APIRouter, BackgroundTasks
from Backend.services.data_ingestion.main import run_data_ingestion

ingestion_router = APIRouter(prefix="/ingestion", tags=["Data Ingestion"])

@ingestion_router.post("/run")
async def trigger_ingestion(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_data_ingestion)
    return {"message": "Data ingestion started 🚀"}
