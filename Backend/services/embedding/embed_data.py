import os
import re
import pandas as pd
from sentence_transformers import SentenceTransformer
import chromadb

# -------------------------------
# CONFIG
# -------------------------------
DATA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/merged_source"
CHROMA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/db/chromadb"
BATCH_SIZE = 100
MAX_WORDS = 250
OVERLAP = 50

# Global state (initialized once)
model = None
client = None
collection = None
CACHE_FILE = None


# -------------------------------
# UTILS
# -------------------------------
def chunk_text(text, max_words=MAX_WORDS, overlap=OVERLAP):
    """Split text into overlapping word chunks."""
    words = re.split(r"\s+", text)
    chunks = []
    for i in range(0, len(words), max_words - overlap):
        chunk = " ".join(words[i:i + max_words]).strip()
        if len(chunk.split()) > 30:
            chunks.append(chunk)
    return chunks


# -------------------------------
# INITIALIZER
# -------------------------------
def initialize_embedder():
    """
    Loads model, chroma client, and initializes global collection.
    Call this ONCE at app startup.
    """
    global model, client, collection, CACHE_FILE

    if model is None:
        model_name = "sentence-transformers/all-MiniLM-L6-v2"
        print(f"🧠 Loading embedding model: {model_name}")
        model = SentenceTransformer(model_name)

    if client is None:
        print(f"🔗 Connecting to ChromaDB at {CHROMA_PATH}")
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_or_create_collection(name="investment_rag")

    CACHE_FILE = os.path.join(CHROMA_PATH, "embedded_ids.txt")
    print("✅ Embedder ready!")


# -------------------------------
# EMBEDDING FUNCTION (CALL FROM ROUTER)
# -------------------------------
def embed_new_data():
    """
    Embeds only new data and stores it in ChromaDB.
    This is the main function you will call from your FastAPI router.
    """
    global model, client, collection, CACHE_FILE

    if model is None:
        initialize_embedder()

    # Load dataset
    print("📂 Loading dataset...")
    df = pd.read_parquet(DATA_PATH)
    print(f"➡️ Loaded {len(df)} records")

    # Load cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            embedded_ids = set(line.strip() for line in f)
    else:
        embedded_ids = set()

    print(f"📁 Cached chunk IDs: {len(embedded_ids)}")

    # Prepare batching
    texts, ids, metas = [], [], []
    new_count = 0

    # Iterate rows
    for i, row in df.iterrows():
        base_id = str(i)

        if base_id in embedded_ids:
            continue

        text = str(row.get("text", "")).strip()
        if not text:
            continue

        company = str(row.get("company", "") or "")
        source = str(row.get("source_type", "") or "")
        date = str(row.get("date", "") or "")

        chunks = chunk_text(text)
        if not chunks:
            continue

        # Create chunks
        for j, chunk in enumerate(chunks):
            chunk_id = f"{base_id}_{j}"
            ids.append(chunk_id)
            texts.append(chunk)
            metas.append({
                "company": company,
                "source": source,
                "date": date,
                "chunk_index": j
            })
            new_count += 1

            # Batch insert
            if len(texts) >= BATCH_SIZE:
                _commit_batch(ids, texts, metas, embedded_ids)
                ids, texts, metas = [], [], []

        # After row-level
        embedded_ids.add(base_id)

    # Commit remaining
    if texts:
        _commit_batch(ids, texts, metas, embedded_ids)

    print(f"✅ Embedded and stored {new_count} new chunks.")
    return {"status": "success", "total_new_chunks": new_count}


# -------------------------------
# INTERNAL BATCH COMMIT
# -------------------------------
def _commit_batch(ids, texts, metas, embedded_ids):
    """Internal helper to embed a batch & update cache file."""
    global model, collection, CACHE_FILE

    embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
    collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)

    # Update cache
    with open(CACHE_FILE, "a") as f:
        for cid in ids:
            f.write(cid + "\n")
