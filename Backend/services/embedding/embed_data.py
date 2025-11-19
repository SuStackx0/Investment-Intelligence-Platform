import os
import json
import uuid
from sentence_transformers import SentenceTransformer
import chromadb

# -------------------------------
# CONFIG
# -------------------------------
DATA_PATH = "/app/outputs/merged_source"   # 👈 folder where 50 JSON files are stored
CHROMA_PATH = "/app/chromadb"
COLLECTION_NAME = "investment_rag"
MAX_CHARS = 1500


# -------------------------------
# GLOBALS
# -------------------------------
client = None
collection = None
model = None


def initialize_embedder():
    global client, collection, model

    # Load embedding model once
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    # Connect to Chroma
    client = chromadb.PersistentClient(path=CHROMA_PATH)

    # Create/get collection
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"}
    )

    print("Initialized embedder + Chroma connection")


# -------------------------------
# Helper: read all JSON files
# -------------------------------
def load_company_files():
    files = []
    for filename in os.listdir(DATA_PATH):
        if filename.endswith(".json"):
            full_path = os.path.join(DATA_PATH, filename)
            with open(full_path, "r") as f:
                data = json.load(f)
            files.append({"filename": filename, "data": data})
    return files


# -------------------------------
# Helper: Chunk text
# -------------------------------
def chunk_text(text, max_chars=MAX_CHARS):
    chunks = []
    text = text.replace("\n", " ").strip()
    for i in range(0, len(text), max_chars):
        chunks.append(text[i: i + max_chars])
    return chunks


# -------------------------------
# MAIN: Embed new JSONs
# -------------------------------
def embed_new_data():
    company_files = load_company_files()
    total_chunks = 0

    for file in company_files:
        company_name = file["filename"]
        json_data = file["data"]

        # convert JSON → string
        text = json.dumps(json_data)

        chunks = chunk_text(text)
        vectors = model.encode(chunks).tolist()

        ids = [f"{company_name}_{uuid.uuid4()}" for _ in chunks]

        collection.add(
            ids=ids,
            embeddings=vectors,
            documents=chunks,
            metadatas=[{"source": company_name}] * len(chunks)
        )

        total_chunks += len(chunks)

    return {
        "companies_processed": len(company_files),
        "chunks_added": total_chunks
    }
