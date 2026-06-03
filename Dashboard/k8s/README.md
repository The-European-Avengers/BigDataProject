# Kubernetes Deployment Guide

## Overview

This guide explains how to deploy the dashboard application to a Kubernetes cluster with Hive integration.

## Prerequisites

1. A Kubernetes cluster (1.19+)
2. `kubectl` configured to access your cluster
3. Docker images built and pushed to a registry
4. Access to Hive server (host, port, credentials)

## Configuration

### 1. Update Docker Registry

Edit `backend.yaml` and `frontend.yaml` and update the image references:

```yaml
image: your-registry/dashboard-backend:latest  # Change 'your-registry'
image: your-registry/dashboard-frontend:latest
```

### 2. Hive Credentials

Edit `secret.yaml` and add your Hive credentials:

```yaml
stringData:
  HIVE_USER: "your-hive-username"
  HIVE_PASSWORD: "your-hive-password"
```

### 3. Update Hive Connection

Edit `configmap.yaml` and set your Hive server details:

```yaml
data:
  HIVE_HOST: "your-hive-server.example.com"
  HIVE_PORT: "10000"
```

### 4. Domain Configuration

Edit `ingress.yaml` and update your domain:

```yaml
- host: dashboard-api.yourdomain.com
- host: dashboard.yourdomain.com
```

## Deployment Steps

### Option 1: Using the deployment script

```bash
cd Dashboard/k8s
chmod +x deploy.sh
./deploy.sh
```

### Option 2: Manual deployment

```bash
# Create namespace
kubectl apply -f namespace.yaml

# Create config and secrets
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml

# Deploy Redis
kubectl apply -f redis.yaml

# Deploy Backend and Frontend
kubectl apply -f backend.yaml
kubectl apply -f frontend.yaml

# Apply Ingress
kubectl apply -f ingress.yaml
```

## Verify Deployment

```bash
# Check pods
kubectl get pods -n dashboard

# Check services
kubectl get services -n dashboard

# Check ingress
kubectl get ingress -n dashboard

# View logs
kubectl logs -f deployment/dashboard-backend -n dashboard
kubectl logs -f deployment/dashboard-frontend -n dashboard
```

## Scaling

### Manual Scaling

```bash
kubectl scale deployment dashboard-backend --replicas=3 -n dashboard
kubectl scale deployment dashboard-frontend --replicas=3 -n dashboard
```

### Auto-scaling

The manifests include HorizontalPodAutoscaler configurations that will automatically scale pods based on CPU and memory usage.

## Environment Variables

### ConfigMap Variables

- `REDIS_HOST`: Redis server hostname (default: redis-service)
- `REDIS_PORT`: Redis server port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `HIVE_HOST`: Hive server hostname
- `HIVE_PORT`: Hive server port (default: 10000)
- `HIVE_DATABASE`: Default Hive database (default: analytics)
- `API_PORT`: Backend API port (default: 3000)
- `CACHE_TTL`: Cache time-to-live in seconds (default: 3600)

### Secret Variables

- `HIVE_USER`: Hive username
- `HIVE_PASSWORD`: Hive password

## API Endpoints

### Predictions Endpoint

```bash
# Get all predictions
curl http://dashboard-api.yourdomain.com/api/hive/predictions?limit=100

# Filter by municipality
curl http://dashboard-api.yourdomain.com/api/hive/predictions?municipality=Copenhagen&limit=100

# Filter by date range
curl http://dashboard-api.yourdomain.com/api/hive/predictions?startDate=2024-01-01&endDate=2024-12-31&limit=100
```

### Tables Endpoint

```bash
# List available tables in analytics database
curl http://dashboard-api.yourdomain.com/api/hive/tables
```

### Query Endpoint

```bash
# Query specific table
curl http://dashboard-api.yourdomain.com/api/hive/table/predictions?limit=50

# Query from different database
curl http://dashboard-api.yourdomain.com/api/hive/table/users?database=default&limit=50
```

### Cache Invalidation

```bash
# Invalidate all cache
curl -X POST http://dashboard-api.yourdomain.com/api/hive/cache/invalidate

# Invalidate specific table cache
curl -X POST http://dashboard-api.yourdomain.com/api/hive/cache/invalidate?table=predictions
```

## Troubleshooting

### Hive Connection Issues

```bash
# Check backend logs for connection errors
kubectl logs deployment/dashboard-backend -n dashboard

# Verify Hive server is accessible
kubectl run -it --rm debug --image=alpine --restart=Never -n dashboard -- \
  telnet HIVE_HOST HIVE_PORT
```

### Redis Connection Issues

```bash
# Check if Redis is running
kubectl get pods -l app=redis -n dashboard

# Check Redis logs
kubectl logs -l app=redis -n dashboard
```

### Pod Resource Issues

```bash
# Check pod resource usage
kubectl top pod -n dashboard

# Check node resources
kubectl top nodes
```

## Cleanup

```bash
# Delete all dashboard resources
kubectl delete namespace dashboard

# Or delete specific deployments
kubectl delete deployment dashboard-backend -n dashboard
kubectl delete deployment dashboard-frontend -n dashboard
kubectl delete deployment redis -n dashboard
```

## Production Considerations

1. **Persistence**: Update Redis to use PersistentVolume for data durability
2. **HTTPS**: Configure cert-manager for automatic SSL certificates
3. **Monitoring**: Add Prometheus metrics and Grafana dashboards
4. **Logging**: Integrate with ELK stack or similar logging solutions
5. **Network Policies**: Add NetworkPolicies to restrict traffic
6. **RBAC**: Implement proper Role-Based Access Control
7. **Resource Limits**: Adjust resource requests/limits based on your load
8. **Image Registry**: Use private registry with proper authentication
