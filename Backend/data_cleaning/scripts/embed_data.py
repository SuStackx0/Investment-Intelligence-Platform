import pandas as pd
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import chromadb
import os
import numpy as np

# -------------------------------
# CONFIG
# -------------------------------
DATA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/data_cleaning/outputs/merged_source/combined_investment_source.parquet"
CHROMA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/chromadb"
BATCH_SIZE = 100  # how many records per commit batch

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
# FIND EXISTING IDS
# -------------------------------
print("🔍 Checking existing entries in ChromaDB...")
existing_count = collection.count()
print(f"📊 Existing embeddings: {existing_count}")

# NOTE: Chroma doesn’t support listing all IDs directly, so we track via local cache
# We'll create a local cache file that remembers which record IDs are embedded

CACHE_FILE = os.path.join(CHROMA_PATH, "embedded_ids.txt")
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        embedded_ids = set(line.strip() for line in f)
else:
    embedded_ids = set()

print(f"📁 Found {len(embedded_ids)} IDs in local cache")

# -------------------------------
# EMBED ONLY NEW DATA
# -------------------------------
texts, ids, metas = [], [], []
new_count = 0

for i, row in tqdm(df.iterrows(), total=len(df), ncols=100, desc="Embedding"):
    doc_id = str(i)
    if doc_id in embedded_ids:
        continue

    text = str(row["text"]).strip()
    if not text:
        continue

    texts.append(text)
    ids.append(doc_id)

    company = str(row.get("company", "") or "")
    source = str(row.get("source_type", "") or "")
    date = str(row.get("date", "") or "")

    metas.append({
        "company": company,
        "source": source,
        "date": date
    })
    new_count += 1

    new_count += 1

    # ---- Batch commit ----
    if len(texts) >= BATCH_SIZE:
        embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)

        # update cache
        with open(CACHE_FILE, "a") as f:
            for _id in ids:
                f.write(_id + "\n")

        texts, ids, metas = [], [], []

# ---- Final leftover ----
if texts:
    embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
    collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)
    with open(CACHE_FILE, "a") as f:
        for _id in ids:
            f.write(_id + "\n")

print(f"✅ {new_count} new records embedded and stored in ChromaDB!")
print(f"📁 DB location: {CHROMA_PATH}")
