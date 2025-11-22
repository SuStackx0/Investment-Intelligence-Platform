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

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Copy App Files
# -----------------------------
COPY . .

# -----------------------------
# Expose Port
# -----------------------------
EXPOSE 8081

# -----------------------------
# Run Server
# -----------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081"]
