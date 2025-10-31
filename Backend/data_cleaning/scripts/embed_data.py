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
BATCH_SIZE = 100  # controls how often to commit embeddings

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
# EMBED + INSERT IN BATCHES
# -------------------------------
print("⚙️ Generating embeddings and inserting in batches...")
texts, ids, metas = [], [], []

for i, row in tqdm(df.iterrows(), total=len(df), ncols=100, desc="Embedding"):
    text = str(row["text"]).strip()
    if not text:
        continue

    texts.append(text)
    ids.append(str(i))
    metas.append({
        "company": row.get("company", ""),
        "source": row.get("source", "")
    })

    # ---- Batch commit ----
    if len(texts) >= BATCH_SIZE:
        embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)
        texts, ids, metas = [], [], []

# ---- Final leftover ----
if texts:
    embs = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
    collection.add(ids=ids, embeddings=embs, documents=texts, metadatas=metas)

print("✅ All data embedded and stored in ChromaDB!")
print(f"📁 DB location: {CHROMA_PATH}")
