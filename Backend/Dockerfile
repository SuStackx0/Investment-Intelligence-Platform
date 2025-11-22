# -----------------------------
# Base Image
# -----------------------------
FROM python:3.10-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

# -----------------------------
# Install Dependencies
# -----------------------------
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements (reuse your LLM/retrieval service deps if needed)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Copy Orchestrator
# -----------------------------
COPY main.py .

# -----------------------------
# Expose Port
# -----------------------------
EXPOSE 8090

# -----------------------------
# Run Orchestrator
# -----------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
