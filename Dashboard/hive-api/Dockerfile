FROM python:3.11-slim

# Install system dependencies for Thrift and SASL
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    libsasl2-modules \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Expose port
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=app.py
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app.py"]
