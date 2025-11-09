from fastapi import APIRouter, Query
from services.retrieval.retrieval_service import retrieve_context
from services.llm.llm_service import get_llm_response

rag_router= APIRouter(prefix="/rag",tags=["RAG"])

@rag_router.get("/query")
async def query_rag(q: str = Query(..., description="User investment question")):
    context = retrieve_context(q)
    answer = get_llm_response(context, q)
    return {"query": q, "answer": answer}