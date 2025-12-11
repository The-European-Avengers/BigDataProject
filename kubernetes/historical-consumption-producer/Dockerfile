FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (lighter since we don't need geopandas)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY producer-historical-consumption.py .

# Set environment variables (can be overridden in K8s)
ENV TOPIC=historical-consumption
ENV BOOTSTRAP_SERVERS=kafka-g5-controller-headless:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV POLL_INTERVAL=86400
ENV RUN_ONCE=false
ENV FORCE_RUN=false

# Run the producer
CMD ["python", "-u", "producer-historical-consumption.py"]