# Use Python 3.11 for compatibility with project dependencies
FROM python:3.11-slim

# Show logs immediately in Docker
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install CA certificates for secure WebSocket connections
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy all producer source files and Avro schemas
COPY . .

# Start all four exchange producers
CMD ["python", "main.py"]