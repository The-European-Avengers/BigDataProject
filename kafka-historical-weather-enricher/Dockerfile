FROM python:3.10-slim-bullseye

# Install Java 11 (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark
ENV SPARK_VERSION=3.4.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark && \
    rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz"

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

# Create application directory and user
RUN useradd -m -u 1001 sparkuser && \
    chown -R sparkuser:sparkuser /home/sparkuser

# Copy Python files and requirements
COPY --chown=sparkuser:sparkuser consumer-historical.py /home/sparkuser/consumer-historical.py
COPY --chown=sparkuser:sparkuser enrichers.py /home/sparkuser/enrichers.py
COPY requirements.txt /home/sparkuser/requirements.txt

# Copy entrypoint script (without chown first, so we can chmod as root)
COPY entrypoint-historical.sh /home/sparkuser/entrypoint-historical.sh

# Make entrypoint executable and then change ownership (must be done as root)
RUN chmod +x /home/sparkuser/entrypoint-historical.sh && \
    chown sparkuser:sparkuser /home/sparkuser/entrypoint-historical.sh

# Install Python dependencies
RUN pip install --no-cache-dir -r /home/sparkuser/requirements.txt

# Set working directory
WORKDIR /home/sparkuser

# Switch to non-root user
USER sparkuser

# Set environment variables with defaults
ENV BOOTSTRAP_SERVERS=kafka-g5:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV CHECKPOINT_ROOT=/tmp/spark/checkpoints/historical_enricher
ENV TRIGGER_INTERVAL="60 seconds"
ENV MUNICIPALITY_CSV_HDFS=hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv
ENV HDFS_NAMENODE=hdfs://namenode-g5:9000

# Expose Spark UI port
EXPOSE 4040

# Use entrypoint script
ENTRYPOINT ["/home/sparkuser/entrypoint-historical.sh"]
