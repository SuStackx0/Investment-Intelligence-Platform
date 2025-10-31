import os
import chromadb
from openai import OpenAI

# -------------------------------
# CONFIG
# -------------------------------
CHROMA_PATH = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/chromadb"
COLLECTION_NAME = "investment_rag"

# 👇 Put your OpenAI API key here (or use env variable)
os.environ["OPENAI_API_KEY"] = ""   # 🔐 replace this

# -------------------------------
# INIT CLIENTS
# -------------------------------
print("🔗 Connecting to ChromaDB...")
chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = chroma_client.get_collection(name=COLLECTION_NAME)
print(f"✅ Connected to Chroma collection: {COLLECTION_NAME}")
print(f"📊 Total stored embeddings: {collection.count()}")

openai_client = OpenAI()

# -------------------------------
# RAG FUNCTION
# -------------------------------
def rag_query(query, n_results=5):
    print("\n🔍 Retrieving relevant context...")
    results = collection.query(query_texts=[query], n_results=n_results)

    contexts = results["documents"][0]
    metadatas = results["metadatas"][0]

    # Build context string for the LLM
    context_text = "\n\n---\n\n".join(
        [f"Source: {m.get('source', '')}, Company: {m.get('company', '')}\n\n{c}"
         for c, m in zip(contexts, metadatas)]
    )

    prompt = f"""
You are a helpful investment research assistant.

Use the following context from various financial data sources to answer the user query.

If the context does not contain enough information, say so concisely.

---

Context:
{context_text}

---

User query: {query}

Answer:
"""

    print("💬 Sending to OpenAI for reasoning...")
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",  # or "gpt-4o" if you prefer more detailed answers
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    answer = response.choices[0].message.content.strip()
    return answer


# -------------------------------
# MAIN LOOP
# -------------------------------
if __name__ == "__main__":
    while True:
        query = input("\n💭 Enter your question (or 'exit'): ").strip()
        if query.lower() in {"exit", "quit"}:
            break

        answer = rag_query(query)
        print("\n🧠 Answer:\n", answer)
