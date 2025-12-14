FROM python:3.10-slim-bullseye

# Install Java 11 and dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark && \
    rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz"

# Download Avro JAR
RUN wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar" \
    -O /opt/spark/jars/spark-avro_2.12-${SPARK_VERSION}.jar

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

# Create user and directories
RUN useradd -m -u 185 spark && \
    mkdir -p /opt/spark/work-dir /home/spark/.ivy2/cache && \
    chown -R spark:spark /opt/spark/work-dir /home/spark

WORKDIR /opt/spark/work-dir

# Install Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=spark:spark src/ ./src/

ENV PYTHONUNBUFFERED=1
ENV DEPLOYMENT_MODE=kubernetes
ENV DATA_BASE_PATH=hdfs://namenode-g5:9000

USER spark