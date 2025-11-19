from fastapi import APIRouter
from embed_data import embed_new_data

router = APIRouter()


@router.post("/run")
def run_embedding():
    """
    Trigger the embedding pipeline.
    Only new records will be embedded.
    """
    result = embed_new_data()
    return {
        "message": "Embedding completed",
        "details": result
    }
