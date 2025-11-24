import chromadb
from sentence_transformers import SentenceTransformer, CrossEncoder
import torch
from common.logging_config import setup_logger

# ------------------------------------------------
# LOGGER
# ------------------------------------------------
logger = setup_logger("retrieval_service")

# ------------------------------------------------
# CONFIG
# ------------------------------------------------
CHROMA_PATH = "/app/chromadb"
COLLECTION_NAME = "investment_rag_test"

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
RERANKER_MODEL = "cross-encoder/ms-marco-TinyBERT-L-2-v2"

# ------------------------------------------------
# GLOBALS
# ------------------------------------------------
embedder = None
reranker = None
collection = None


# ------------------------------------------------
# INITIALIZATION
# ------------------------------------------------
def initialize_retriever():
    global embedder, reranker, collection

    try:
        device = "mps" if torch.backends.mps.is_available() else "cpu"
        logger.info(f"Selected device for reranker: {device}")

        logger.info(f"🧠 Loading embedding model: {EMBED_MODEL}")
        embedder = SentenceTransformer(EMBED_MODEL)
        logger.info("Embedding model loaded successfully.")

        logger.info(f"🔁 Loading reranker model: {RERANKER_MODEL}")
        reranker = CrossEncoder(RERANKER_MODEL, device=device)
        logger.info("Reranker model loaded successfully.")

        logger.info(f"🔗 Connecting to ChromaDB at {CHROMA_PATH}")
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_or_create_collection(COLLECTION_NAME)
        logger.info("ChromaDB connection established.")

        logger.info("✅ Retriever initialization complete.")

    except Exception as e:
        logger.exception(f"❌ Error initializing retriever: {e}")
        raise


# ------------------------------------------------
# RETRIEVAL PIPELINE
# ------------------------------------------------
def retrieve_context(query: str, prefetch_k: int = 20, final_k: int = 5):
    logger.info(f"Received retrieval query: '{query}' (prefetch={prefetch_k}, final={final_k})")

    if embedder is None or reranker is None or collection is None:
        error_msg = "❌ Retriever not initialized. Call initialize_retriever() at startup."
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    try:
        # Step 1: Embed query
        query_embedding = embedder.encode(query)
        logger.info("Query embedding completed.")

        # Step 2: Query Chroma
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=prefetch_k
        )
        logger.info("Chroma returned results.")

        docs = results.get("documents", [[]])[0] or []
        logger.info(f"Documents retrieved: {len(docs)}")

        if len(docs) == 0:
            logger.warning("No relevant documents found.")
            return "No relevant documents found."

        # Step 3: Rerank
        pairs = [(query, doc) for doc in docs]
        scores = reranker.predict(pairs)
        logger.info("Reranking completed.")

        reranked = sorted(zip(docs, scores), key=lambda x: x[1], reverse=True)
        top_docs = [doc for doc, _ in reranked[:final_k]]

        context = "\n\n".join(top_docs)
        logger.info("Final context built successfully.")

        return context[:1500]

    except Exception as e:
        logger.exception(f"❌ Retrieval pipeline failed: {e}")
        return "Error during retrieval — check logs."
