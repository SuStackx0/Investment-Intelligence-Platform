import chromadb
from sentence_transformers import SentenceTransformer
from openai import OpenAI

# -------------------------------
# CONFIG
# -------------------------------
CHROMA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/chromadb"
COLLECTION_NAME = "investment_rag"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 5  # how many chunks to retrieve
OPENAI_MODEL = "gpt-4-turbo"  # or "gpt-4o-mini"

# -------------------------------
# INIT OPENAI CLIENT (put your key here)
# -------------------------------
client_oa = OpenAI(api_key="")

# -------------------------------
# INIT MODEL & CHROMA
# -------------------------------
print("🔗 Connecting to ChromaDB...")
client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = client.get_collection(COLLECTION_NAME)
print(f"✅ Connected to collection: {COLLECTION_NAME}")

model = SentenceTransformer(MODEL_NAME)
print(f"🧠 Loaded embedding model: {MODEL_NAME}")

# -------------------------------
# QUERY LOOP
# -------------------------------
while True:
    query = input("\n💭 Enter your question (or 'exit'): ").strip()
    if query.lower() == "exit":
        break

    print("🔍 Retrieving top chunks...")

    # Embed query and search
    query_emb = model.encode(query, convert_to_numpy=True, normalize_embeddings=True)
    results = collection.query(
        query_embeddings=[query_emb],
        n_results=TOP_K,
        include=["documents", "metadatas", "distances"]
    )

    # Collect retrieved text
    retrieved_chunks = []
    for doc, meta, dist in zip(
        results["documents"][0], results["metadatas"][0], results["distances"][0]
    ):
        retrieved_chunks.append(f"[{meta.get('company', 'N/A')} | {meta.get('source', 'N/A')}] {doc}")

    context = "\n\n".join(retrieved_chunks)

    # -------------------------------
    # CALL OPENAI LLM
    # -------------------------------
    print("🤖 Generating answer with OpenAI...")

    prompt = f"""You are a financial analysis assistant.
Use the context below (retrieved from investment data) to answer the user's question.

QUESTION:
{query}

CONTEXT:
{context}
Dont answer from your own knowledge.Only answer from the context.If the context doesnt have answer just say "i dont know"
Respond in a clear, factual way. If something is uncertain, say so explicitly.

"""

    response = client_oa.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "You are a knowledgeable financial assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
    )

    answer = response.choices[0].message.content
    print("\n💬 LLM Response:\n")
    print(answer)
    print("\n" + "="*80)
