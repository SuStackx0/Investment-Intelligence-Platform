from fastapi import FastAPI, Request
from retrieval_router import router as retrieval_router
from retrieval_service import initialize_retriever
from common.logging_config import setup_logger   # <--- ADD THIS

# ------------------------------------------------
# LOGGER
# ------------------------------------------------
logger = setup_logger("retrieval_service")

# ------------------------------------------------
# FASTAPI APP
# ------------------------------------------------
app = FastAPI(
    title="Retrieval Service API",
    description="API for retrieving context from ChromaDB using embedding + reranking",
    version="1.0"
)

# ------------------------------------------------
# REQUEST LOGGING MIDDLEWARE
# ------------------------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming Request → {request.method} {request.url}")

    try:
        response = await call_next(request)
        logger.info(f"Response → {response.status_code}")
        return response
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        raise

# ------------------------------------------------
# STARTUP EVENT
# ------------------------------------------------
@app.on_event("startup")
def startup_event():
    logger.info("🚀 Initializing retriever...")

    try:
        initialize_retriever()
        logger.info("✅ Retriever loaded successfully.")
    except Exception as e:
        logger.exception(f"❌ Retriever initialization failed: {e}")
        raise

# ------------------------------------------------
# ROUTERS
# ------------------------------------------------
app.include_router(retrieval_router, prefix="/retrieve", tags=["Retrieval"])

# ------------------------------------------------
# ROOT ENDPOINT
# ------------------------------------------
