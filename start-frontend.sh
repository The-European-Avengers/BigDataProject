#!/bin/bash

# HDFS Dashboard Frontend Startup Script
# Runs frontend in foreground with visible output

cd "$(dirname "$0")/Dashboard/frontend" || exit 1

echo "🚀 Starting HDFS Dashboard Frontend..."
echo "📁 Working directory: $(pwd)"
echo ""

# Kill any existing process on port 3001
if lsof -Pi :3001 -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "⚠️  Port 3001 is in use, killing existing process..."
  lsof -ti:3001 | xargs kill -9 2>/dev/null || true
  sleep 1
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
  echo "📦 Installing dependencies..."
  npm install
fi

echo ""
echo "✅ Frontend is starting on http://localhost:3001"
echo "🛑 Press Ctrl+C to stop"
echo ""

# Run in foreground with explicit port
PORT=3001 npm start
