from fastapi import APIRouter
from pydantic import BaseModel
from retrieval_service import retrieve_context


router = APIRouter()

class QueryRequest(BaseModel):
    query: str
    prefetch_k: int = 20
    final_k: int = 5


@router.post("/")
def retrieve(req: QueryRequest):
    """
    Retrieve context for a user query via embedding + reranking.
    """
    result = retrieve_context(req.query, req.prefetch_k, req.final_k)
    return {"query": req.query, "context": result}
