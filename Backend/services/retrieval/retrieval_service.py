import chromadb
from sentence_transformers import SentenceTransformer, CrossEncoder
import torch

# -------------------------------
# CONFIG
# -------------------------------
CHROMA_PATH = "/app/chromadb"

COLLECTION_NAME = "investment_rag_test"

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
RERANKER_MODEL = "cross-encoder/ms-marco-TinyBERT-L-2-v2"

# -------------------------------
# GLOBALS
# -------------------------------
embedder = None
reranker = None
collection = None


# -------------------------------
# INITIALIZATION
# -------------------------------
def initialize_retriever():
    global embedder, reranker, collection

    # Select device for reranker (CrossEncoder)
    device = "mps" if torch.backends.mps.is_available() else "cpu"

    print("🧠 Loading embedding model:", EMBED_MODEL)
    embedder = SentenceTransformer(EMBED_MODEL)

    print("🔁 Loading reranker model:", RERANKER_MODEL)
    reranker = CrossEncoder(RERANKER_MODEL, device=device)

    print("🔗 Connecting to ChromaDB at:", CHROMA_PATH)
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = client.get_or_create_collection(COLLECTION_NAME)

    print("✅ Retriever initialization complete")


# -------------------------------
# RETRIEVAL PIPELINE
# -------------------------------
def retrieve_context(query: str, prefetch_k: int = 20, final_k: int = 5):
    """
    Step 1: Retrieve top prefetch_k docs using embeddings
    Step 2: Rerank them using CrossEncoder scores
    Step 3: Return top final_k stitched into 1 context
    """

    if embedder is None or reranker is None or collection is None:
        raise RuntimeError("❌ Retriever not initialized. Call initialize_retriever() at startup.")

    # Step 1: Embed query
    query_embedding = embedder.encode(query)

    # Step 2: Query from Chroma
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=prefetch_k
    )

    docs = results.get("documents", [[]])[0] or []
    if len(docs) == 0:
        return "No relevant documents found."

    # Step 3: Rerank
    pairs = [(query, doc) for doc in docs]
    scores = reranker.predict(pairs)

    reranked = sorted(zip(docs, scores), key=lambda x: x[1], reverse=True)
    top_docs = [doc for doc, _ in reranked[:final_k]]

    # Build final context
    context = "\n\n".join(top_docs)

    # Limit size
    return context[:1500]
