from fastapi import FastAPI
from pydantic import BaseModel
import requests

# -------------------------------
# CONFIG
# -------------------------------
RETRIEVAL_URL = "http://retrieval_service:8070/retrieve/"  # use container name in compose
LLM_URL = "http://llm_service:8081/llm/"  # container name in compose

app = FastAPI(title="Orchestrator Service")

class QueryRequest(BaseModel):
    query: str
    prefetch_k: int = 20
    final_k: int = 5

# -------------------------------
# ORCHESTRATOR ENDPOINT
# -------------------------------
@app.post("/ask/")
def ask_user(req: QueryRequest):
    # Step 1: Call Retrieval service
    try:
        retrieval_resp = requests.post(
            RETRIEVAL_URL,
            json={
                "query": req.query,
                "prefetch_k": req.prefetch_k,
                "final_k": req.final_k
            },
            timeout=20
        ).json()
    except Exception as e:
        return {"error": f"Failed to fetch context: {e}"}

    context = retrieval_resp.get("context", "")

    # Step 2: Call LLM service
    try:
        llm_resp = requests.post(
            LLM_URL,
            json={
                "query": req.query,
                "context": context
            },
            timeout=60
        ).json()
    except Exception as e:
        return {"error": f"Failed to get LLM response: {e}"}

    return {"query": req.query, "context": context, "answer": llm_resp.get("answer", "")}
