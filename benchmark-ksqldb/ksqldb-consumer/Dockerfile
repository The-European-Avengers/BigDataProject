# Use a lightweight base image with bash support
FROM alpine:latest

# Install any dependencies your script might need
# Common ones for ksqlDB operations:
RUN apk add --no-cache \
    bash \
    curl \
    jq \
    coreutils

# Set working directory
WORKDIR /app

# Copy the script into the container
COPY run-ksqldb-benchmark.sh /app/

# Make the script executable
RUN chmod +x /app/run-ksqldb-benchmark.sh

# Set the entrypoint to run the script
ENTRYPOINT ["/app/run-ksqldb-benchmark.sh"]