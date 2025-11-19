# -------------------------------
# Base Image
# -------------------------------
FROM python:3.10-slim

# -------------------------------
# Install system dependencies
# -------------------------------
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Set work directory
# -------------------------------
WORKDIR /app

# -------------------------------
# Install Python dependencies
# -------------------------------
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# -------------------------------
# Copy application files
# -------------------------------
COPY . .

# -------------------------------
# Expose FastAPI port
# -------------------------------
EXPOSE 8000

# -------------------------------
# Run server
# -------------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


# docker run -d \
#   --name embedding_service \
#   -p 8030:8000 \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/merged_source:/app/outputs/merged_source" \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/db/chromadb:/app/chromadb" \
#   embedding_service
