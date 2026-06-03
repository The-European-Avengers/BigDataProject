# Hive REST API

A Flask-based REST API for querying Apache Hive.

## Features

- Health check endpoint
- List all databases
- List tables in a database
- Execute custom Hive queries
- Get data from specific tables with pagination

## API Endpoints

### Health Check
```
GET /health
```

### Get Databases
```
GET /databases
```

### Get Tables
```
GET /tables?database=analytics
```

### Execute Query
```
POST /query
Content-Type: application/json

{
  "query": "SELECT * FROM my_table LIMIT 10",
  "database": "analytics"
}
```

### Get Table Data
```
GET /table/<database>/<table>?limit=100&offset=0
```

## Running with Docker

### Build and Run
```bash
docker-compose up --build
```

### Environment Variables
- `HIVE_HOST`: Hive server hostname (default: localhost)
- `HIVE_PORT`: Hive server port (default: 10000)
- `HIVE_USERNAME`: Hive username (default: root)
- `HIVE_DATABASE`: Default database (default: default)

## Running Locally

### Prerequisites
Make sure your Hive port-forward is running:
```bash
kubectl port-forward svc/hive-server 10000:10000
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run the Application
```bash
python app.py
```

The API will be available at `http://localhost:5000`

## Testing

### Test Health Check
```bash
curl http://localhost:5000/health
```

### Test Get Databases
```bash
curl http://localhost:5000/databases
```

### Test Get Tables
```bash
curl http://localhost:5000/tables?database=analytics
```

### Test Query
```bash
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SHOW TABLES", "database": "analytics"}'
```
