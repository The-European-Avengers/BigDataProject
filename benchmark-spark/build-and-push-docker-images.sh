#!/bin/bash
# ================================================================
# BUILD AND PUSH DOCKER IMAGES TO DOCKER HUB
# ================================================================
# This script builds all three Docker images and pushes them to Docker Hub
# ================================================================

set -e

# ================================================================
# CONFIGURATION
# ================================================================

# Your Docker Hub username (CHANGE THIS!)
DOCKER_USERNAME=${DOCKER_USERNAME:-"ortemiy"}

# Image names
SPARK_CONSUMER_IMAGE="spark-consumer"
PRODUCER_IMAGE="benchmark-producer"
LATENCY_MONITOR_IMAGE="latency-monitor"

# Version tag
VERSION=${VERSION:-"latest"}

# Platforms to build for (multi-arch support)
PLATFORMS=${PLATFORMS:-"linux/amd64"}  # Add ,linux/arm64 for Apple Silicon

echo "=========================================="
echo "  DOCKER IMAGE BUILD & PUSH"
echo "=========================================="
echo "Docker Hub Username: $DOCKER_USERNAME"
echo "Version Tag: $VERSION"
echo "Platforms: $PLATFORMS"
echo "=========================================="
echo ""

# ================================================================
# STEP 1: VERIFY PREREQUISITES
# ================================================================
echo "📋 Step 1: Checking prerequisites..."
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed"
    echo "   Install Docker from: https://docs.docker.com/get-docker/"
    exit 1
fi

echo "  ✅ Docker is installed: $(docker --version)"

# Check if logged into Docker Hub
if ! docker info | grep -q "Username: $DOCKER_USERNAME"; then
    echo ""
    echo "  ⚠️  Not logged into Docker Hub as $DOCKER_USERNAME"
    echo "  Attempting login..."
    docker login
    echo ""
fi

echo "  ✅ Logged into Docker Hub"
echo ""

# Check if buildx is available (for multi-platform builds)
if docker buildx version &> /dev/null; then
    echo "  ✅ Docker Buildx available (multi-platform support enabled)"
    USE_BUILDX=true
else
    echo "  ⚠️  Docker Buildx not available (single platform build only)"
    USE_BUILDX=false
fi

echo ""

# ================================================================
# STEP 2: VERIFY PROJECT STRUCTURE
# ================================================================
echo "📁 Step 2: Verifying project structure..."
echo ""

# Check Spark Consumer project
if [ ! -f "spark-consumer/Dockerfile" ]; then
    echo "❌ Error: spark-consumer/Dockerfile not found"
    exit 1
fi
echo "  ✅ Spark Consumer project found"

# Check Producer project
if [ ! -f "producer/Dockerfile" ]; then
    echo "❌ Error: producer/Dockerfile not found"
    exit 1
fi
echo "  ✅ Producer project found"

# Check Latency Monitor project
if [ ! -f "latency-monitor/Dockerfile" ]; then
    echo "❌ Error: latency-monitor/Dockerfile not found"
    exit 1
fi
echo "  ✅ Latency Monitor project found"

echo ""

# ================================================================
# STEP 3: BUILD SPARK CONSUMER IMAGE
# ================================================================
echo "🔨 Step 3: Building Spark Consumer image..."
echo ""

cd spark-consumer

if [ "$USE_BUILDX" = true ]; then
    # Build with buildx (multi-platform)
    docker buildx build \
        --platform $PLATFORMS \
        --tag $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:$VERSION \
        --tag $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:latest \
        --push \
        .
else
    # Standard build (single platform)
    docker build \
        --tag $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:$VERSION \
        --tag $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:latest \
        .
fi

cd ..

echo ""
echo "  ✅ Spark Consumer image built"
echo "     - $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:$VERSION"
echo "     - $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:latest"
echo ""

# ================================================================
# STEP 4: BUILD PRODUCER IMAGE
# ================================================================
echo "🔨 Step 4: Building Producer image..."
echo ""

cd producer

if [ "$USE_BUILDX" = true ]; then
    docker buildx build \
        --platform $PLATFORMS \
        --tag $DOCKER_USERNAME/$PRODUCER_IMAGE:$VERSION \
        --tag $DOCKER_USERNAME/$PRODUCER_IMAGE:latest \
        --push \
        .
else
    docker build \
        --tag $DOCKER_USERNAME/$PRODUCER_IMAGE:$VERSION \
        --tag $DOCKER_USERNAME/$PRODUCER_IMAGE:latest \
        .
fi

cd ..

echo ""
echo "  ✅ Producer image built"
echo "     - $DOCKER_USERNAME/$PRODUCER_IMAGE:$VERSION"
echo "     - $DOCKER_USERNAME/$PRODUCER_IMAGE:latest"
echo ""

# ================================================================
# STEP 5: BUILD LATENCY MONITOR IMAGE
# ================================================================
echo "🔨 Step 5: Building Latency Monitor image..."
echo ""

cd latency-monitor

if [ "$USE_BUILDX" = true ]; then
    docker buildx build \
        --platform $PLATFORMS \
        --tag $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:$VERSION \
        --tag $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:latest \
        --push \
        .
else
    docker build \
        --tag $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:$VERSION \
        --tag $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:latest \
        .
fi

cd ..

echo ""
echo "  ✅ Latency Monitor image built"
echo "     - $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:$VERSION"
echo "     - $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:latest"
echo ""

# ================================================================
# STEP 6: PUSH IMAGES TO DOCKER HUB (if not using buildx)
# ================================================================
if [ "$USE_BUILDX" = false ]; then
    echo "📤 Step 6: Pushing images to Docker Hub..."
    echo ""

    echo "  Pushing Spark Consumer..."
    docker push $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:$VERSION
    docker push $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:latest
    echo "  ✅ Spark Consumer pushed"

    echo ""
    echo "  Pushing Producer..."
    docker push $DOCKER_USERNAME/$PRODUCER_IMAGE:$VERSION
    docker push $DOCKER_USERNAME/$PRODUCER_IMAGE:latest
    echo "  ✅ Producer pushed"

    echo ""
    echo "  Pushing Latency Monitor..."
    docker push $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:$VERSION
    docker push $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:latest
    echo "  ✅ Latency Monitor pushed"

    echo ""
else
    echo "📤 Step 6: Images already pushed (via buildx)"
    echo ""
fi

# ================================================================
# STEP 7: VERIFY IMAGES
# ================================================================
echo "✅ Step 7: Verifying local images..."
echo ""

docker images | grep -E "$DOCKER_USERNAME/($SPARK_CONSUMER_IMAGE|$PRODUCER_IMAGE|$LATENCY_MONITOR_IMAGE)" || echo "  (Images not found locally - this is OK if using buildx with --push)"

echo ""

# ================================================================
# STEP 8: GENERATE DEPLOYMENT FILES
# ================================================================
echo "📝 Step 8: Generating deployment files..."
echo ""

# Create k8s directory if it doesn't exist
mkdir -p k8s

# Update the main deployment file with correct image names
cat > k8s/image-config.yaml <<EOF
# Image Configuration
# Generated by build-and-push-docker-images.sh
# Date: $(date)

# Update these values in your Kubernetes manifests:
DOCKER_REGISTRY: $DOCKER_USERNAME
SPARK_CONSUMER_IMAGE: $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:$VERSION
PRODUCER_IMAGE: $DOCKER_USERNAME/$PRODUCER_IMAGE:$VERSION
LATENCY_MONITOR_IMAGE: $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:$VERSION
EOF

echo "  ✅ Image configuration saved to k8s/image-config.yaml"
echo ""

# ================================================================
# FINAL SUMMARY
# ================================================================
echo "=========================================="
echo "  ✅ ALL IMAGES BUILT AND PUSHED"
echo "=========================================="
echo ""
echo "Images on Docker Hub:"
echo "  1. $DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE:$VERSION"
echo "  2. $DOCKER_USERNAME/$PRODUCER_IMAGE:$VERSION"
echo "  3. $DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE:$VERSION"
echo ""
echo "Next Steps:"
echo "  1. Update Kubernetes manifests with image names:"
echo "     sed -i 's|your-registry|$DOCKER_USERNAME|g' k8s/*.yaml"
echo ""
echo "  2. Or export environment variable:"
echo "     export DOCKER_REGISTRY=$DOCKER_USERNAME"
echo ""
echo "  3. Deploy to Kubernetes:"
echo "     ./run-k8s-benchmark.sh 100"
echo ""
echo "  4. Or deploy ksqlDB:"
echo "     ./run-ksqldb-benchmark.sh 100"
echo ""
echo "View images on Docker Hub:"
echo "  https://hub.docker.com/r/$DOCKER_USERNAME/$SPARK_CONSUMER_IMAGE"
echo "  https://hub.docker.com/r/$DOCKER_USERNAME/$PRODUCER_IMAGE"
echo "  https://hub.docker.com/r/$DOCKER_USERNAME/$LATENCY_MONITOR_IMAGE"
echo ""
echo "=========================================="
