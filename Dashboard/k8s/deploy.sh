#!/bin/bash

# Kubernetes Deployment Script for Dashboard

set -e

NAMESPACE="dashboard"
REGISTRY="your-registry"  # Change to your Docker registry
BACKEND_IMAGE="$REGISTRY/dashboard-backend"
FRONTEND_IMAGE="$REGISTRY/dashboard-frontend"
TAG="latest"

echo "🚀 Starting Dashboard Kubernetes Deployment..."

# Step 1: Create namespace
echo "📦 Creating namespace..."
kubectl apply -f namespace.yaml

# Step 2: Create ConfigMap and Secret
echo "⚙️  Applying ConfigMap and Secret..."
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml

# Note: Update secret.yaml with your Hive credentials
echo "⚠️  Please update secret.yaml with your Hive credentials and re-apply:"
echo "   kubectl apply -f secret.yaml"

# Step 3: Deploy Redis
echo "🔴 Deploying Redis..."
kubectl apply -f redis.yaml

# Step 4: Wait for Redis to be ready
echo "⏳ Waiting for Redis to be ready..."
kubectl wait --for=condition=ready pod -l app=redis -n $NAMESPACE --timeout=300s || true

# Step 5: Deploy Backend
echo "🔵 Deploying Backend..."
kubectl apply -f backend.yaml

# Step 6: Deploy Frontend
echo "🟣 Deploying Frontend..."
kubectl apply -f frontend.yaml

# Step 7: Apply Ingress
echo "🌐 Applying Ingress..."
echo "⚠️  Please update ingress.yaml with your domain and apply:"
echo "   kubectl apply -f ingress.yaml"

# Step 8: Check deployment status
echo "✅ Deployment started!"
echo ""
echo "Checking deployment status..."
kubectl get deployments -n $NAMESPACE
kubectl get services -n $NAMESPACE
kubectl get pods -n $NAMESPACE

echo ""
echo "📝 Next steps:"
echo "1. Update your Docker registry in backend.yaml and frontend.yaml"
echo "2. Build and push Docker images:"
echo "   docker build -t $BACKEND_IMAGE:$TAG ./backend"
echo "   docker push $BACKEND_IMAGE:$TAG"
echo "   docker build -t $FRONTEND_IMAGE:$TAG ./frontend"
echo "   docker push $FRONTEND_IMAGE:$TAG"
echo "3. Update secret.yaml with your Hive credentials"
echo "4. Update ingress.yaml with your domain"
echo "5. Apply ingress: kubectl apply -f ingress.yaml"
echo ""
echo "To check pod logs:"
echo "   kubectl logs -f deployment/dashboard-backend -n $NAMESPACE"
echo "   kubectl logs -f deployment/dashboard-frontend -n $NAMESPACE"
