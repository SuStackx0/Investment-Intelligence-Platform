from fastapi import FastAPI, Request
from embedding_router import router as embedding_router
from embed_data import initialize_embedder
from common.logging_config import setup_logger   # <--- ADD THIS

# ------------------------------------------------
# LOGGER
# ------------------------------------------------
logger = setup_logger("embedding_service")

# ------------------------------------------------
# FASTAPI APP
# ------------------------------------------------
app = FastAPI(
    title="Embedding Service API",
    description="API for embedding documents into ChromaDB",
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
        logger.exception(f"Unhandled error: {e}")
        raise

# ------------------------------------------------
# STARTUP EVENT
# ------------------------------------------------
@app.on_event("startup")
def startup_event():
    logger.info("🚀 Initializing embedder...")

    try:
        initialize_embedder()
        logger.info("✅ Embedder loaded successfully.")
    except Exception as e:
        logger.exception(f"❌ Failed to initialize embedder: {e}")
        raise

# ------------------------------------------------
# ROUTER INCLUDE
# ------------------------------------------------
app.include_router(embedding_router, prefix="/embedding", tags=["Embedding"])

# ------------------------------------------------
# ROOT ENDPOINT
# ------------------------------------------------
@app.get("/")
def root():
    logger.info("Root endpoint hit.")
    return {"status": "ok", "message": "Embedding API is running"}
