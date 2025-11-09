import chromadb
from sentence_transformers import SentenceTransformer, CrossEncoder
import torch

CHROMA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/db/chromadb"
COLLECTION_NAME = "investment_rag"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
RERANKER_MODEL = "cross-encoder/ms-marco-TinyBERT-L-2-v2"

device = "mps" if torch.backends.mps.is_available() else "cpu"

# Load embedding model (for vector search)
embedder = SentenceTransformer(EMBED_MODEL)

# Load reranker (for deeper semantic filtering)
reranker = CrossEncoder(RERANKER_MODEL, device=device)


client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = client.get_or_create_collection(COLLECTION_NAME)

def retrieve_context(query: str, prefetch_k: int = 20, final_k: int = 5):
    """
    Step 1: Retrieve top prefetch_k docs from Chroma via embeddings
    Step 2: Rerank them using CrossEncoder
    Step 3: Return top final_k combined as context
    """
    
    query_embedding = embedder.encode([query])[0]
    results = collection.query(query_embeddings=[query_embedding], n_results=prefetch_k)

    docs = results["documents"][0]
    if not docs:
        return "No relevant documents found in the database."

    # Step 2: Rerank the docs based on semantic relevance
    pairs = [(query, doc) for doc in docs]
    rerank_scores = reranker.predict(pairs)

    
    reranked = sorted(zip(docs, rerank_scores), key=lambda x: x[1], reverse=True)
    top_docs = [doc for doc, _ in reranked[:final_k]]

    
    return "\n\n".join(top_docs)
