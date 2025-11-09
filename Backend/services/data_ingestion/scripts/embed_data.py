import os
import re
import pandas as pd
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import chromadb

# -------------------------------
# CONFIG
# -------------------------------
DATA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/merged_source"
CHROMA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/db/chromadb"
BATCH_SIZE = 100  # number of chunks per batch
MAX_WORDS = 250
OVERLAP = 50

# -------------------------------
# UTILS
# -------------------------------
def chunk_text(text, max_words=MAX_WORDS, overlap=OVERLAP):
    """Split text into overlapping word chunks."""
    words = re.split(r"\s+", text)
    chunks = []
    for i in range(0, len(words), max_words - overlap):
        chunk = " ".join(words[i:i + max_words]).strip()
        if len(chunk.split()) > 30:  # skip too-small chunks
            chunks.append(chunk)
    return chunks


# -------------------------------
# LOAD DATA
# -------------------------------
print("📂 Loading dataset...")
df = pd.read_parquet(DATA_PATH)
print(f"✅ Loaded {len(df)} records")

# -------------------------------
# LOAD MODEL
# -------------------------------
model_name = "sentence-transformers/all-MiniLM-L6-v2"
print(f"🧠 Loading model: {model_name}")
model = SentenceTransformer(model_name)

# -------------------------------
# INIT CHROMA
# -------------------------------
client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = client.get_or_create_collection(name="investment_rag")

# -------------------------------
# LOCAL CACHE
# -------------------------------
CACHE_FILE = os.path.join(CHROMA_PATH, "embedded_ids.txt")
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        embedded_ids = set(line.strip() for line in f)
else:
    embedded_ids = set()

print(f"📁 Found {len(embedded_ids)} cached IDs")

# -------------------------------
# EMBED ONLY NEW DATA
# -------------------------------
texts, ids, metas = [], [], []
new_count = 0

for i, row in tqdm(df.iterrows(), total=len(df), ncols=100, desc="Embedding"):
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

        # ---- Batch commit ----
        if len(texts) >= BATCH_SIZE:
            embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
            collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)

            # Update cache
            with open(CACHE_FILE, "a") as f:
                for cid in ids:
                    f.write(cid + "\n")

            texts, ids, metas = [], [], []

# ---- Final leftover ----
if texts:
    embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
    collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)
    with open(CACHE_FILE, "a") as f:
        for cid in ids:
            f.write(cid + "\n")

print(f"✅ Embedded and stored {new_count} new chunks in ChromaDB!")
print(f"📁 DB location: {CHROMA_PATH}")
