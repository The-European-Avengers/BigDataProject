#!/bin/bash
set -e

# Energy ML Predictor Spark Job Deployment Script

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
DOCKER_REGISTRY="${DOCKER_REGISTRY:-your-registry}"
IMAGE_NAME="${IMAGE_NAME:-energy-ml-predictor}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
NAMESPACE="${NAMESPACE:-default}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Energy ML Predictor Spark Job Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    print_info "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "docker not found"
        exit 1
    fi
    
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl not found"
        exit 1
    fi
    
    print_info "✓ Prerequisites satisfied"
}

check_spark_operator() {
    print_info "Checking Spark Operator..."
    
    if ! kubectl get crd sparkapplications.sparkoperator.k8s.io &> /dev/null; then
        print_warning "Spark Operator not found"
        read -p "Install Spark Operator? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_spark_operator
        else
            print_error "Spark Operator required for Kubernetes deployment"
            exit 1
        fi
    else
        print_info "✓ Spark Operator installed"
    fi
}

install_spark_operator() {
    print_info "Installing Spark Operator..."
    kubectl apply -f https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/releases/download/v1beta2-1.3.8-3.1.1/spark-operator.yaml
    
    print_info "Waiting for Spark Operator to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=spark-operator -n spark-operator --timeout=300s
    
    print_info "✓ Spark Operator installed"
}

build_image() {
    print_info "Building Docker image..."
    
    FULL_IMAGE="${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
    
    docker build -t "${FULL_IMAGE}" .
    
    if [ $? -eq 0 ]; then
        print_info "✓ Image built: ${FULL_IMAGE}"
    else
        print_error "Failed to build image"
        exit 1
    fi
}

push_image() {
    print_info "Pushing Docker image..."
    
    FULL_IMAGE="${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
    
    docker push "${FULL_IMAGE}"
    
    if [ $? -eq 0 ]; then
        print_info "✓ Image pushed"
    else
        print_error "Failed to push image"
        exit 1
    fi
}

create_namespace() {
    if [ "${NAMESPACE}" != "default" ]; then
        print_info "Creating namespace ${NAMESPACE}..."
        kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
    fi
}

deploy_k8s() {
    print_info "Deploying to Kubernetes..."
    
    FULL_IMAGE="${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
    
    print_info "Creating RBAC..."
    kubectl apply -f k8s/spark-rbac.yaml -n "${NAMESPACE}"
    
    print_info "Creating PersistentVolumeClaim..."
    kubectl apply -f k8s/persistent-volume.yaml -n "${NAMESPACE}"
    
    print_info "Creating ConfigMap..."
    kubectl apply -f k8s/configmap.yaml -n "${NAMESPACE}"
    
    print_info "Creating ScheduledSparkApplication..."
    cat k8s/cronjob.yaml | \
        sed "s|your-registry/energy-ml-predictor:latest|${FULL_IMAGE}|g" | \
        kubectl apply -f - -n "${NAMESPACE}"
    
    print_info "✓ Deployment complete"
}

check_status() {
    print_info "Checking deployment status..."
    echo ""
    
    print_info "ScheduledSparkApplications:"
    kubectl get scheduledsparkapplications -n "${NAMESPACE}"
    echo ""
    
    print_info "Recent SparkApplications:"
    kubectl get sparkapplications -n "${NAMESPACE}" --sort-by=.metadata.creationTimestamp
    echo ""
    
    print_info "PersistentVolumeClaims:"
    kubectl get pvc -n "${NAMESPACE}" energy-data-pvc
}

run_manual_job() {
    print_info "Creating manual Spark job..."
    
    FULL_IMAGE="${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
    
    # Check if already exists
    if kubectl get sparkapplication energy-ml-predictor-manual -n "${NAMESPACE}" &> /dev/null; then
        print_warning "Manual job already exists. Deleting..."
        kubectl delete sparkapplication energy-ml-predictor-manual -n "${NAMESPACE}"
        sleep 2
    fi
    
    cat k8s/job.yaml | \
        sed "s|your-registry/energy-ml-predictor:latest|${FULL_IMAGE}|g" | \
        kubectl apply -f - -n "${NAMESPACE}"
    
    print_info "✓ Manual job created"
    
    print_info "Monitor with: kubectl logs -f energy-ml-predictor-manual-driver -n ${NAMESPACE}"
}

view_logs() {
    print_info "Fetching Spark application logs..."
    
    # Find the latest driver pod
    DRIVER_POD=$(kubectl get pods -n "${NAMESPACE}" \
        -l spark-role=driver,app=energy-ml-predictor \
        --sort-by=.metadata.creationTimestamp \
        -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
    
    if [ -z "$DRIVER_POD" ]; then
        print_error "No driver pod found"
        return
    fi
    
    print_info "Streaming logs from $DRIVER_POD..."
    kubectl logs -f "$DRIVER_POD" -n "${NAMESPACE}"
}

run_local_test() {
    print_info "Running local test..."
    
    if [ ! -d "./consumption" ]; then
        print_error "Local data directories not found"
        print_info "Expected structure: ./consumption/, ./weather/, ./forecast/"
        return
    fi
    
    python -m src.main --mode local --training-years 2
}

show_menu() {
    echo ""
    echo "What would you like to do?"
    echo "1) Build Docker image"
    echo "2) Push Docker image"
    echo "3) Check Spark Operator"
    echo "4) Deploy to Kubernetes"
    echo "5) Full deployment (build + push + deploy)"
    echo "6) Check deployment status"
    echo "7) Run manual job"
    echo "8) View logs"
    echo "9) Run local test"
    echo "10) Exit"
    echo ""
    read -p "Enter choice [1-10]: " choice
    
    case $choice in
        1)
            check_prerequisites
            build_image
            show_menu
            ;;
        2)
            push_image
            show_menu
            ;;
        3)
            check_spark_operator
            show_menu
            ;;
        4)
            check_spark_operator
            create_namespace
            deploy_k8s
            show_menu
            ;;
        5)
            check_prerequisites
            check_spark_operator
            build_image
            push_image
            create_namespace
            deploy_k8s
            check_status
            show_menu
            ;;
        6)
            check_status
            show_menu
            ;;
        7)
            run_manual_job
            show_menu
            ;;
        8)
            view_logs
            show_menu
            ;;
        9)
            run_local_test
            show_menu
            ;;
        10)
            print_info "Exiting..."
            exit 0
            ;;
        *)
            print_error "Invalid choice"
            show_menu
            ;;
    esac
}

# Main
if [ $# -eq 0 ]; then
    show_menu
else
    case $1 in
        build)
            check_prerequisites
            build_image
            ;;
        push)
            push_image
            ;;
        deploy)
            check_spark_operator
            create_namespace
            deploy_k8s
            ;;
        all)
            check_prerequisites
            check_spark_operator
            build_image
            push_image
            create_namespace
            deploy_k8s
            check_status
            ;;
        status)
            check_status
            ;;
        manual)
            run_manual_job
            ;;
        logs)
            view_logs
            ;;
        local)
            run_local_test
            ;;
        *)
            print_error "Unknown command: $1"
            echo "Usage: $0 [build|push|deploy|all|status|manual|logs|local]"
            exit 1
            ;;
    esac
fi