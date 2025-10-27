from fastapi import FastAPI, APIRouter

pipeline_router = APIRouter(prefix="/pipeline", tags=["pipeline"])

@pipeline_router.post("/input-query/")
async def input_query(text: str):
    return {"message": "Query received", "query": text}
