# HDFS Dashboard

A full-stack dashboard application to visualize and explore data stored in HDFS through Hive, with Redis caching for optimal performance.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   React Frontend                     │
│              (Port 3001)                            │
└────────────────┬──────────────────────────────────────┘
                 │ HTTP/REST
┌────────────────┴──────────────────────────────────────┐
│                 NestJS Backend                        │
│              (Port 3000)                             │
└────────────────┬──────────────────────────────────────┘
                 │
        ┌────────┴────────┐
        │                 │
┌───────┴──────┐  ┌──────┴──────┐
│   Redis      │  │   Hive      │
│  (Cache)     │  │ (Data)      │
└──────────────┘  └──────┬──────┘
                         │
                  ┌──────┴──────┐
                  │    HDFS     │
                  │  (Storage)  │
                  └─────────────┘
```

## Project Structure

```
Dashboard/
├── backend/                    # NestJS Backend
│   ├── src/
│   │   ├── modules/
│   │   │   ├── redis/         # Redis caching service
│   │   │   └── hive/          # Hive query service
│   │   ├── app.module.ts
│   │   ├── app.service.ts
│   │   ├── app.controller.ts
│   │   └── main.ts
│   ├── package.json
│   ├── tsconfig.json
│   ├── .env                   # Configuration file
│   └── README.md
│
├── frontend/                   # React Frontend
│   ├── src/
│   │   ├── components/        # React components
│   │   ├── pages/             # Page components
│   │   ├── services/          # API services
│   │   ├── App.js
│   │   └── index.js
│   ├── public/
│   │   └── index.html
│   ├── package.json
│   └── README.md
│
├── docker-compose.yml         # Docker compose for easy deployment
└── README.md                  # This file
```

## Quick Start

### Prerequisites

- Node.js 18+
- npm or yarn
- Redis running (or use Docker)
- Hive server configured

### Backend Setup

```bash
cd Dashboard/backend

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Run in development
npm run start:dev

# Or build and run production
npm run build
npm run start:prod
```

Backend will be available at `http://localhost:3000`

### Frontend Setup

```bash
cd Dashboard/frontend

# Install dependencies
npm install

# Configure environment
echo "REACT_APP_API_URL=http://localhost:3000" > .env

# Run in development
npm start

# Build for production
npm run build
```

Frontend will be available at `http://localhost:3001`

## Docker Deployment

```bash
# Start both backend and frontend
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## API Endpoints

### Health Check

- `GET /health` - Server health status

### Hive Tables

- `GET /api/hive/tables` - List all Hive tables (cached)
- `GET /api/hive/table/:tableName` - Query specific table data (cached)
- `POST /api/hive/cache/invalidate` - Invalidate cache for specific table

## Environment Configuration

### Backend (.env)

```env
# Redis
REDIS_HOST=redis-service.bd-bd-gr-05.svc.cluster.local
REDIS_PORT=6379
REDIS_DB=0

# Hive
HIVE_HOST=hive-server.bd-bd-gr-05.svc.cluster.local
HIVE_PORT=10000
HIVE_DATABASE=default

# Application
API_PORT=3000
CACHE_TTL=3600
```

### Frontend (.env)

```env
REACT_APP_API_URL=http://localhost:3000
```

## Features

- 📊 **Data Exploration**: Browse and explore Hive tables from HDFS
- ⚡ **Redis Caching**: Automatic caching with configurable TTL
- 🔄 **Cache Management**: Invalidate cache on demand
- 📱 **Responsive Design**: Works on desktop and mobile
- 🎨 **Modern UI**: Clean and intuitive interface
- 🚀 **Performance**: Optimized for large datasets

## Development

### Backend Development

```bash
cd Dashboard/backend

# Watch mode
npm run start:dev

# Run tests
npm test

# Run linter
npm run lint
```

### Frontend Development

```bash
cd Dashboard/frontend

# Start dev server with hot reload
npm start

# Run tests
npm test

# Build production
npm run build
```

## Kubernetes Deployment

To deploy on your Kubernetes cluster:

```bash
# Backend
kubectl apply -f kubernetes/dashboard-backend-deployment.yaml

# Frontend
kubectl apply -f kubernetes/dashboard-frontend-deployment.yaml
```

## Troubleshooting

### Backend won't start

- Check Redis is running: `redis-cli ping`
- Verify Hive connection settings in `.env`
- Check logs: `npm run start:dev`

### Frontend shows "Backend not connected"

- Ensure backend is running on port 3000
- Check `REACT_APP_API_URL` in frontend `.env`
- Clear browser cache and reload

### Cache not working

- Verify Redis is accessible from backend
- Check Redis configuration in `.env`
- Use `POST /api/hive/cache/invalidate` to clear cache

## Performance Tips

1. **Increase cache TTL** for stable datasets
2. **Use table-specific queries** instead of full table scans
3. **Monitor Redis memory** to prevent eviction
4. **Implement pagination** for large result sets
5. **Use indexes** on frequently queried columns in Hive

## Next Steps

1. Implement actual Hive connection in `HiveService`
2. Add authentication/authorization
3. Implement pagination for large datasets
4. Add data export functionality
5. Create Kubernetes deployment manifests
6. Set up monitoring and logging

## License

UNLICENSE
