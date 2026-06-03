# React Frontend

HDFS Dashboard frontend built with React.

## Setup

```bash
cd Dashboard/frontend
npm install
```

## Configuration

Create a `.env` file:

```
REACT_APP_API_URL=http://localhost:3000
```

## Running

Development:

```bash
npm start
```

This will start the frontend on `http://localhost:3001`.

Build for production:

```bash
npm run build
```

## Features

- 📋 Browse Hive tables
- 📊 View table data with pagination
- ⚡ Automatic caching with Redis
- 🔄 Cache invalidation
- 📱 Responsive design
