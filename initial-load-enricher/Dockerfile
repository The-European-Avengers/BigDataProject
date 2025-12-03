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

# Download Avro JAR for Spark (required for writing AVRO files)
RUN wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.4.1/spark-avro_2.12-3.4.1.jar" \
    -O /opt/spark/jars/spark-avro_2.12-3.4.1.jar

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

# Create application directory and user
RUN useradd -m -u 1001 sparkuser && \
    mkdir -p /home/sparkuser/data && \
    chown -R sparkuser:sparkuser /home/sparkuser

# Copy Python batch enrichment script
COPY --chown=sparkuser:sparkuser batch_enrichment.py /home/sparkuser/batch_enrichment.py

# Copy municipality data
COPY --chown=sparkuser:sparkuser data/municipality_codes_to_coordinates.csv /home/sparkuser/data/municipality_codes_to_coordinates.csv

# Copy entrypoint script
COPY --chown=sparkuser:sparkuser entrypoint-batch.sh /home/sparkuser/entrypoint-batch.sh

# Install Python dependencies
COPY requirements.txt /home/sparkuser/requirements.txt
RUN pip install --no-cache-dir -r /home/sparkuser/requirements.txt

# Make entrypoint executable
RUN chmod +x /home/sparkuser/entrypoint-batch.sh

# Set working directory
WORKDIR /home/sparkuser

# Switch to non-root user
USER sparkuser

# Set environment variables with defaults
ENV HDFS_NAMENODE=hdfs://namenode-g5:9000
ENV MUNICIPALITY_CSV=/home/sparkuser/data/municipality_codes_to_coordinates.csv
ENV HADOOP_CONF_fs_defaultFS=hdfs://namenode-g5:9000

# Expose Spark UI port
EXPOSE 4040

# Run the batch enrichment job via entrypoint
ENTRYPOINT ["/home/sparkuser/entrypoint-batch.sh"]