from fastapi import FastAPI
from retrieval_router import router as retrieval_router
from retrieval_service import initialize_retriever

app = FastAPI(
    title="Retrieval Service API",
    description="API for retrieving context from ChromaDB using embedding + reranking",
    version="1.0"
)

# -------------------------------
# STARTUP EVENT
# -------------------------------
@app.on_event("startup")
def startup_event():
    print("🚀 Initializing retriever...")
    initialize_retriever()
    print("✅ Retriever loaded.")

# -------------------------------
# ROUTERS
# -------------------------------
app.include_router(retrieval_router, prefix="/retrieve", tags=["Retrieval"])

# -------------------------------
# ROOT
# -------------------------------
@app.get("/")
def root():
    return {"status": "ok", "message": "Retrieval API is running"}
