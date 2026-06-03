#!/bin/bash

# Wait for Docker to be ready
echo "Waiting for Docker to start..."
while ! docker ps > /dev/null 2>&1; do
  sleep 1
done
echo "✅ Docker is ready"

# Build backend image
echo "Building dashboard-backend image..."
cd /Users/alknific/Documents/FAKS/BigDataProject/Dashboard/backend
docker build -t dashboard-backend:latest .
if [ $? -eq 0 ]; then
  echo "✅ Backend image built successfully"
else
  echo "❌ Failed to build backend image"
  exit 1
fi

# Build frontend image
echo "Building dashboard-frontend image..."
cd /Users/alknific/Documents/FAKS/BigDataProject/Dashboard/frontend
docker build -t dashboard-frontend:latest .
if [ $? -eq 0 ]; then
  echo "✅ Frontend image built successfully"
else
  echo "❌ Failed to build frontend image"
  exit 1
fi

echo ""
echo "✅ All images built successfully!"
echo ""
echo "Pods should start now. Check status with:"
echo "  kubectl get pods -n bd-bd-gr-05"
