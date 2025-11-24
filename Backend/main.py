from fastapi import FastAPI
from pydantic import BaseModel
import requests
import logging
import os

# -------------------------------
# LOGGING SETUP
# -------------------------------
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/orchestrator.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("OrchestratorService")

# -------------------------------
# CONFIG
# -------------------------------
RETRIEVAL_URL = "http://retrieval_service:8070/retrieve/"
LLM_URL = "http://llm_service:8081/generate"

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
    logger.info(f"Received query: {req.query}")

    # ------------------------------------
    # Step 1: Call Retrieval Service
    # ------------------------------------
    try:
        logger.info("Calling Retrieval Service...")

        retrieval_resp = requests.post(
            RETRIEVAL_URL,
            json={
                "query": req.query,
                "prefetch_k": req.prefetch_k,
                "final_k": req.final_k
            },
            timeout=20
        ).json()

        logger.info("Retrieval Service response received successfully")

    except Exception as e:
        logger.error(f"Retrieval Service call failed: {e}", exc_info=True)
        return {"error": f"Failed to fetch context: {str(e)}"}

    context = retrieval_resp.get("context", "")
    logger.info(f"Context length received: {len(context)} chars")

    # ------------------------------------
    # Step 2: Call LLM Service
    # ------------------------------------
    try:
        logger.info("Calling LLM Service...")

        llm_resp = requests.post(
            LLM_URL,
            json={
                "query": req.query,
                "context": context
            },
            timeout=60
        ).json()

        logger.info("LLM Service response received successfully")

    except Exception as e:
        logger.error(f"LLM Service call failed: {e}", exc_info=True)
        return {"error": f"Failed to get LLM response: {str(e)}"}

    final_answer = llm_resp.get("answer", "")

    logger.info("Returning final answer to user")

    return {
        "query": req.query,
        "context": context,
        "answer": final_answer
    }


@app.get("/")
def root():
    logger.info("Health check ping received")
    return {"status": "ok", "message": "Orchestrator API running"}
