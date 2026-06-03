# Dashboard Updates: Hive Analytics Integration & K8s Deployment

## Overview

The dashboard has been updated with real Hive integration to query the `analytics.predictions` table and is now ready for Kubernetes deployment.

## What's Changed

### 1. **Backend Updates**

#### Dependencies Added

- `hive-driver`: For Hive server connection
- `thrift`: Required by Hive driver

#### Hive Service Implementation (`src/modules/hive/hive.service.ts`)

- **Real Hive Connection**: Connects to Hive using configured host/port/credentials
- **getPredictions()**: New method to query `analytics.predictions` table with filtering
  - Filter by municipality
  - Filter by date range (startDate, endDate)
  - Configurable limit
- **queryTable()**: Generic table query with database/table name parameters
- **getTables()**: List available tables in analytics database
- **Connection Pooling**: Graceful connection handling with reconnection support
- **Redis Caching**: All queries are cached to reduce load on Hive

#### New Predictions Endpoint (`GET /api/hive/predictions`)

```
GET /api/hive/predictions?limit=100
GET /api/hive/predictions?municipality=Copenhagen&limit=100
GET /api/hive/predictions?startDate=2024-01-01&endDate=2024-12-31&limit=100
```

#### Response Format

```json
{
  "tableName": "analytics.predictions",
  "limit": 100,
  "rowCount": 50,
  "columns": ["municipality", "date", "prediction_value", "..."],
  "rows": [
    {
      "municipality": "Copenhagen",
      "date": "2024-01-01",
      "prediction_value": 42.5
    }
  ]
}
```

### 2. **Configuration**

#### Environment Variables (`.env`)

```
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
HIVE_HOST=localhost
HIVE_PORT=10000
HIVE_DATABASE=analytics
HIVE_USER=your_username
HIVE_PASSWORD=your_password
API_PORT=3000
CACHE_TTL=3600
NODE_ENV=development
```

#### New ConfigMap in K8s (`k8s/configmap.yaml`)

Defines all configuration variables for K8s deployments.

#### New Secret in K8s (`k8s/secret.yaml`)

Stores sensitive credentials (Hive username/password).

### 3. **Docker & K8s**

#### Updated Dockerfile

- Multi-stage build for smaller image size
- Health check endpoint configured
- Production-optimized
- Listens on `0.0.0.0` for K8s compatibility

#### New K8s Deployment Files

- **namespace.yaml**: Creates `dashboard` namespace
- **configmap.yaml**: Configuration for all services
- **secret.yaml**: Secure credential storage
- **redis.yaml**: Redis Deployment + Service with health checks
- **backend.yaml**: Backend Deployment + Service + HPA (auto-scaling)
- **frontend.yaml**: Frontend Deployment + Service + HPA
- **ingress.yaml**: Ingress routing for domains
- **deploy.sh**: Automated deployment script
- **README.md**: Comprehensive K8s deployment guide

## Quick Start

### Local Development

1. **Update environment variables**:

```bash
cd Dashboard/backend
# Edit .env with your Hive server details
nano .env
```

2. **Install dependencies**:

```bash
npm install
```

3. **Run locally**:

```bash
npm run start:dev
```

4. **Test the predictions endpoint**:

```bash
curl http://localhost:3000/api/hive/predictions?limit=10
```

### Docker Compose (Local)

```bash
cd Dashboard
docker-compose up --build
```

The application will be available at:

- Backend: http://localhost:3000
- Frontend: http://localhost:3001

### Kubernetes Deployment

1. **Prepare your environment**:

```bash
cd Dashboard/k8s

# Edit configuration files with your settings
nano configmap.yaml  # Set HIVE_HOST, HIVE_PORT
nano secret.yaml     # Set HIVE_USER, HIVE_PASSWORD
nano backend.yaml    # Update Docker registry
nano frontend.yaml   # Update Docker registry
nano ingress.yaml    # Update domain names
```

2. **Build and push Docker images**:

```bash
docker build -t your-registry/dashboard-backend:latest ./backend
docker push your-registry/dashboard-backend:latest

docker build -t your-registry/dashboard-frontend:latest ./frontend
docker push your-registry/dashboard-frontend:latest
```

3. **Deploy to K8s**:

```bash
./deploy.sh
# Or manually:
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f redis.yaml
kubectl apply -f backend.yaml
kubectl apply -f frontend.yaml
kubectl apply -f ingress.yaml
```

4. **Verify deployment**:

```bash
kubectl get pods -n dashboard
kubectl get services -n dashboard
kubectl logs -f deployment/dashboard-backend -n dashboard
```

## API Endpoints

### Predictions (New)

```bash
# Get all predictions
curl http://localhost:3000/api/hive/predictions?limit=100

# Filter by municipality
curl http://localhost:3000/api/hive/predictions?municipality=Copenhagen&limit=100

# Filter by date range
curl http://localhost:3000/api/hive/predictions?startDate=2024-01-01&endDate=2024-12-31&limit=100
```

### Tables

```bash
# List tables in analytics database
curl http://localhost:3000/api/hive/tables
```

### Query Generic Table

```bash
# Query table from analytics database
curl http://localhost:3000/api/hive/table/predictions?limit=50

# Query table from different database
curl http://localhost:3000/api/hive/table/users?database=default&limit=50
```

### Cache Management

```bash
# Invalidate all cache
curl -X POST http://localhost:3000/api/hive/cache/invalidate

# Invalidate specific table cache
curl -X POST http://localhost:3000/api/hive/cache/invalidate?table=predictions
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│              React Frontend                      │
│           (Port 3001 / K8s Service)            │
└────────────────┬──────────────────────────────────┘
                 │ HTTP/REST
┌────────────────┴──────────────────────────────────┐
│            NestJS Backend                         │
│          (Port 3000 / K8s Service)              │
│    ├─ Hive Service (Real Connection)            │
│    └─ Redis Module (Caching)                    │
└────────────────┬──────────────────────────────────┘
                 │
        ┌────────┴────────┐
        │                 │
┌───────┴──────┐  ┌──────┴──────┐
│   Redis      │  │   Hive      │
│  (Cache)     │  │  (Analytics)│
└──────────────┘  └──────┬──────┘
                         │
                  ┌──────┴──────┐
                  │    HDFS     │
                  │  (Storage)  │
                  └─────────────┘
```

## Production Considerations

### Security

- ✅ Secrets managed via K8s Secrets
- ✅ CORS configured for specific domains
- ✅ Health checks prevent crashes
- ⚠️ Consider NetworkPolicies for network isolation
- ⚠️ Use private Docker registry with auth

### Performance

- ✅ Redis caching for Hive queries (TTL: 3600s)
- ✅ Multi-replica deployments
- ✅ Horizontal Pod Autoscaler configured
- ✅ Resource limits defined

### Reliability

- ✅ Liveness & readiness probes
- ✅ Health check endpoint
- ✅ Graceful connection handling
- ✅ Rolling updates with zero downtime

### Monitoring

- ⚠️ Add Prometheus metrics
- ⚠️ Add centralized logging (ELK, Loki)
- ⚠️ Add Grafana dashboards

## Troubleshooting

### Hive Connection Issues

```bash
# Check backend logs
kubectl logs -f deployment/dashboard-backend -n dashboard

# Test Hive connectivity
kubectl run -it --rm debug --image=alpine --restart=Never -n dashboard -- \
  nc -zv HIVE_HOST HIVE_PORT
```

### Redis Connection Issues

```bash
# Check Redis pod
kubectl get pods -l app=redis -n dashboard

# Check Redis logs
kubectl logs -l app=redis -n dashboard
```

### Pod Not Starting

```bash
# Check pod status
kubectl describe pod POD_NAME -n dashboard

# Check events
kubectl get events -n dashboard --sort-by='.lastTimestamp'
```

## Files Modified/Created

### Modified

- `backend/package.json` - Added Hive dependencies
- `backend/.env` - Added Hive configuration
- `backend/Dockerfile` - Production-optimized
- `backend/src/main.ts` - Improved CORS handling
- `backend/src/modules/hive/hive.service.ts` - Real Hive integration
- `backend/src/modules/hive/hive.controller.ts` - Added predictions endpoint

### Created

- `k8s/namespace.yaml`
- `k8s/configmap.yaml`
- `k8s/secret.yaml`
- `k8s/redis.yaml`
- `k8s/backend.yaml`
- `k8s/frontend.yaml`
- `k8s/ingress.yaml`
- `k8s/deploy.sh`
- `k8s/README.md`
- `HIVE_INTEGRATION.md` (this file)

## Next Steps

1. **Configure Hive Connection**:

   - Set `HIVE_HOST`, `HIVE_PORT` in k8s/configmap.yaml
   - Set `HIVE_USER`, `HIVE_PASSWORD` in k8s/secret.yaml

2. **Build Docker Images**:

   - Build and push to your registry
   - Update image references in k8s manifests

3. **Deploy to K8s**:

   - Configure domain in k8s/ingress.yaml
   - Run `k8s/deploy.sh`

4. **Monitor & Scale**:

   - Watch pod logs
   - Monitor resource usage
   - Adjust HPA settings if needed

5. **Add Monitoring**:
   - Set up Prometheus for metrics
   - Configure Grafana dashboards
   - Set up alerting rules

## Support

For issues or questions about the Hive integration:

1. Check backend logs: `kubectl logs -f deployment/dashboard-backend -n dashboard`
2. Verify Hive connectivity
3. Check Redis cache status
4. Review the K8s deployment guide in `k8s/README.md`
