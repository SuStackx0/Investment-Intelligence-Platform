FROM python:3.11-slim

# ------------------------------
# System dependencies
# ------------------------------
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------
# Create app directory
# ------------------------------
WORKDIR /app

# ------------------------------
# Install Python packages
# ------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------
# Copy app files
# ------------------------------
COPY . .

# ------------------------------
# Expose port
# ------------------------------
EXPOSE 8002

# ------------------------------
# Run API
# ------------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
