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

# Copy JAR from builder
COPY --from=builder /app/target/scala-3.3.1/latency-monitor.jar /app/latency-monitor.jar


# Create output directory
RUN mkdir -p /app/benchmark-results

# Environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV INPUT_TOPIC=weather.aggregated.output
ENV OUTPUT_DIR=/app/benchmark-results
ENV MAX_WAIT_TIME_MS=30000
ENV MAX_EMPTY_POLLS=5

# Entry point
ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "/app/latency-monitor.jar"]

# Default argument
CMD ["100"]