from fastapi import FastAPI
from embedding_router import router as embedding_router
from embed_data import initialize_embedder

app = FastAPI(
    title="Embedding Service API",
    description="API for embedding documents into ChromaDB",
    version="1.0"
)


# -------------------------------
# STARTUP
# -------------------------------
@app.on_event("startup")
def startup_event():
    print("🚀 Initializing embedder...")
    initialize_embedder()
    print("✅ Embedder loaded.")


# -------------------------------
# ROUTERS
# -------------------------------
app.include_router(embedding_router, prefix="/embedding", tags=["Embedding"])


# -------------------------------
# ROOT
# -------------------------------
@app.get("/")
def root():
    return {"status": "ok", "message": "Embedding API is running"}
