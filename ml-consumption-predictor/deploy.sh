#!/bin/bash

set -e

# Configuration
REGISTRY="registry.gitlab.sdu.dk/the-european-avengers/bigdataproject"
IMAGE_NAME="energy-ml-predictor"
VERSION="${1:-latest}"
NAMESPACE="default"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Energy ML Predictor - Deployment${NC}"
echo -e "${GREEN}================================${NC}"
echo ""

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl not found. Please install kubectl."
    exit 1
fi

# Check if docker is available
if ! command -v docker &> /dev/null; then
    print_error "docker not found. Please install docker."
    exit 1
fi

# Function to build Docker image
build_image() {
    print_info "Building Docker image..."
    
    # Build Docker image (source code is copied via Dockerfile)
    docker build -t ${REGISTRY}/${IMAGE_NAME}:${VERSION} .
    
    if [ $? -eq 0 ]; then
        print_info "✓ Docker image built successfully"
    else
        print_error "Failed to build Docker image"
        exit 1
    fi
}

# Function to push Docker image
push_image() {
    print_info "Pushing Docker image to registry..."
    
    docker push ${REGISTRY}/${IMAGE_NAME}:${VERSION}
    
    if [ $? -eq 0 ]; then
        print_info "✓ Docker image pushed successfully"
    else
        print_error "Failed to push Docker image"
        exit 1
    fi
}

# Function to deploy to Kubernetes
deploy_k8s() {
    print_info "Deploying to Kubernetes..."
    
    # Apply RBAC and ConfigMap
    kubectl apply -f k8s/deployment.yaml
    
    if [ $? -eq 0 ]; then
        print_info "✓ Kubernetes resources created successfully"
    else
        print_error "Failed to deploy to Kubernetes"
        exit 1
    fi
}

# Function to check job status
check_status() {
    print_info "Checking job status..."
    
    kubectl get jobs -n ${NAMESPACE} -l app=energy-ml-predictor
    echo ""
    kubectl get pods -n ${NAMESPACE} -l app=energy-ml-predictor
}

# Function to view logs
view_logs() {
    print_info "Fetching logs..."
    
    POD_NAME=$(kubectl get pods -n ${NAMESPACE} -l app=energy-ml-predictor,job=dec2024 -o jsonpath='{.items[0].metadata.name}')
    
    if [ -z "$POD_NAME" ]; then
        print_warn "No pod found for job"
        return
    fi
    
    kubectl logs -n ${NAMESPACE} -f ${POD_NAME}
}

# Function to delete job
delete_job() {
    print_info "Deleting job..."
    
    kubectl delete job energy-ml-predictor-dec2024 -n ${NAMESPACE}
    
    if [ $? -eq 0 ]; then
        print_info "✓ Job deleted successfully"
    else
        print_warn "Job may not exist"
    fi
}

# Main menu
case "${2:-all}" in
    build)
        build_image
        ;;
    push)
        push_image
        ;;
    deploy)
        deploy_k8s
        ;;
    status)
        check_status
        ;;
    logs)
        view_logs
        ;;
    delete)
        delete_job
        ;;
    all)
        build_image
        push_image
        deploy_k8s
        echo ""
        print_info "Deployment complete!"
        print_info "Check status with: $0 ${VERSION} status"
        print_info "View logs with: $0 ${VERSION} logs"
        ;;
    *)
        echo "Usage: $0 [VERSION] [COMMAND]"
        echo ""
        echo "VERSION: Docker image version tag (default: latest)"
        echo ""
        echo "COMMANDS:"
        echo "  build   - Build Docker image only"
        echo "  push    - Push Docker image to registry"
        echo "  deploy  - Deploy to Kubernetes"
        echo "  status  - Check job status"
        echo "  logs    - View job logs"
        echo "  delete  - Delete job"
        echo "  all     - Build, push, and deploy (default)"
        echo ""
        echo "Examples:"
        echo "  $0                    # Build, push, and deploy with 'latest' tag"
        echo "  $0 v1.0.0 all        # Build, push, and deploy with 'v1.0.0' tag"
        echo "  $0 latest build      # Only build image"
        echo "  $0 latest logs       # View logs"
        exit 1
        ;;
esac

echo ""
print_info "Done!"