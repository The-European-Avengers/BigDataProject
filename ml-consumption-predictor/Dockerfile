# Spark application Docker image
FROM apache/spark-py:v3.5.0

USER root

# Set working directory
WORKDIR /opt/spark/work-dir

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/work-dir:$PYTHONPATH

# Download Spark Avro package (will be added to classpath)
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar

# Run as spark user
USER 185

# Default command (will be overridden by spark-submit)
ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["python", "-m", "src.main"]