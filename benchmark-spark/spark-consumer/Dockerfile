# Multi-stage build - Stage 1: Build with SBT
FROM eclipse-temurin:11-jdk AS builder

# Install SBT
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY build.sbt .
COPY project ./project
COPY src ./src

# Build fat JAR
RUN sbt clean assembly

# Runtime stage
FROM eclipse-temurin:11-jre-jammy

WORKDIR /app

# Install necessary libraries for Spark
RUN apt-get update && \
    apt-get install -y procps && \
    rm -rf /var/lib/apt/lists/*

# Copy JAR from builder
COPY --from=builder /app/target/scala-3.3.7/spark-consumer.jar /app/consumer.jar

# Create necessary directories
RUN mkdir -p /tmp/spark/checkpoints && \
    mkdir -p /app/logs

# Environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV THROUGHPUT=100
ENV SPARK_LOCAL_DIRS=/tmp/spark
ENV SPARK_LOG_DIR=/app/logs
# Entry point
ENTRYPOINT ["java", "-Xmx4g", "-Xms2g", "-jar", "/app/consumer.jar"]

# Default argument
CMD ["100"]