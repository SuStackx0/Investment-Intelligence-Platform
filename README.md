# 💹 Intelligent Investment Platform (RAG-Based)

*A fully containerized microservice architecture for real-time investment question answering.*

---

## 🚀 Overview

The **Intelligent Investment Platform** is a complete Retrieval-Augmented Generation (RAG) system built with:

* **FastAPI** microservices
* **ChromaDB** for vector storage
* **Sentence Transformers** for embeddings
* **TinyLlama** / Llama models for LLM inference
* **Docker Compose** for orchestration
* **Streamlit** for a clean user interface

It allows users to ask **any market or investment-related question** and returns AI-powered, context-aware insights.

---

## 🧩 Architecture

The project is broken into small, independent services:

```
Streamlit UI  →  Orchestrator API  →  Retrieval Service
                                       ↙         ↘
                          Embedding Service     LLM Service
                                  ↖
                              Data Ingestion
```

### **1. Data Ingestion Service**

* Fetches raw company data, news, filings, stock data
* Cleans, merges and stores results
* Outputs structured chunks for embeddings

### **2. Embedding Service**

* Generates vector embeddings using `sentence-transformers/all-MiniLM-L6-v2`
* Stores them in **ChromaDB**

### **3. Retrieval Service**

* Performs vector search + reranking
* Extracts the most relevant context for the query

### **4. LLM Service**

* Loads the TinyLlama (or chosen LLM) model
* Generates final answers using retrieved context

### **5. Orchestrator Service**

* Exposes `/ask` endpoint
* Coordinates retrieval + LLM
* Returns final JSON answer to frontend

### **6. Streamlit Web App**

* Simple, clean UI for asking questions
* Displays AI insights
* Stores chat history

---

## 🐳 Docker Compose (Multi-Service Deployment)

Run all services using:

```bash
docker compose up --build
```

This will start:

* `data_ingestion_service`
* `embedding_service`
* `retrieval_service`
* `llm_service`
* `orchestrator_service`
* All FastAPI app servers with logging
* Mounted volumes for outputs/logs/ChromaDB

---

## 💻 Running the Streamlit Frontend

```bash
streamlit run app.py
```

It will run at:

```
http://localhost:8501
```

---

## 🔥 Features

### ✔ Complete RAG Pipeline

From ingestion → embedding → retrieval → generation

### ✔ Microservice Architecture

Each component is isolated and scalable independently.

### ✔ High-Quality Logging

Every service writes detailed logs to disk, including:

* Errors
* Success logs
* Model loading events
* Inference details

### ✔ ChromaDB Vector Store

Persistent vector search with mounted volume support.

### ✔ LLM Response Coherence

Context sent to LLM is reranked and filtered.

### ✔ Clean Streamlit UI

* Chat-like interface
* History
* Error handling
* Smooth UX

---

## 📦 API Usage

### **Orchestrator Endpoint**

```http
POST /ask
```

### Payload Example

```json
{
  "query": "What is happening with Reliance Industries stock?",
  "prefetch_k": 20,
  "final_k": 5
}
```

### Sample Response

```json
{
  "query": "...",
  "context": "...top documents...",
  "answer": "AI-generated investment insight..."
}
```

---

## 📁 Project Structure

```
Backend/
  ├── services/
  │   ├── data_ingestion/
  │   ├── embedding/
  │   ├── retrieval/
  │   ├── llm/
  │   └── orchestrator/
  ├── db/
  │   └── chromadb/
Frontend/
  └── streamlit/
Dockerfile
docker-compose.yml
```

---

## 🛠 Tech Stack

**Backend**

* Python
* FastAPI
* ChromaDB
* Sentence Transformers
* Llama/TinyLlama
* PyTorch

**Frontend**

* Streamlit

**Infra**

* Docker
* Docker Compose

---

## 🧪 Future Enhancements

* Add model streaming (token-by-token response)
* Support PDF ingestion
* Add FinBERT sentiment scoring
* Add cron-based scheduled ingestion
* Add authentication (JWT)

---

## ❤️ Contributing

Feel free to create issues or PRs.

---
