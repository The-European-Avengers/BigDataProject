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
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark && \
    rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz"

# Create application directory and user
RUN useradd -m -u 1001 sparkuser && \
    mkdir -p /home/sparkuser/data && \
    chown -R sparkuser:sparkuser /home/sparkuser

# Copy Python files
COPY --chown=sparkuser:sparkuser consumer.py /home/sparkuser/consumer.py
COPY --chown=sparkuser:sparkuser enrichers.py /home/sparkuser/enrichers.py
COPY --chown=sparkuser:sparkuser schemas.py /home/sparkuser/schemas.py
COPY --chown=sparkuser:sparkuser entrypoint.sh /home/sparkuser/entrypoint.sh

# Copy municipality data
COPY --chown=sparkuser:sparkuser data/municipality_codes_to_coordinates.csv /home/sparkuser/data/municipality_codes_to_coordinates.csv

# Install Python dependencies
COPY requirements.txt /home/sparkuser/requirements.txt
RUN pip install --no-cache-dir -r /home/sparkuser/requirements.txt

# Make entrypoint executable
RUN chmod +x /home/sparkuser/entrypoint.sh

# Set working directory
WORKDIR /home/sparkuser

# Switch to non-root user
USER sparkuser

# Set environment variables with defaults
ENV BOOTSTRAP_SERVERS=kafka:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV CHECKPOINT_ROOT=/tmp/spark/checkpoints/kafka_enricher
ENV TRIGGER_INTERVAL="10 seconds"
ENV MUNICIPALITY_CSV=/home/sparkuser/data/municipality_codes_to_coordinates.csv

# Expose Spark UI port
EXPOSE 4040

# Use entrypoint script
ENTRYPOINT ["/home/sparkuser/entrypoint.sh"]