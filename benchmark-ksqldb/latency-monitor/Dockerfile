# Stage 1: Build
FROM eclipse-temurin:17-jdk AS builder

RUN apt-get update && apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN sbt clean assembly

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /app/target/scala-*/latency-monitor.jar /app/latency-monitor.jar
COPY data /app/data

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081

ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "/app/latency-monitor.jar"]
CMD ["1000"]
