#!/bin/bash

# HDFS Dashboard Backend Startup Script
# Runs backend in foreground with visible output

cd "$(dirname "$0")/Dashboard/backend" || exit 1

echo "🚀 Starting HDFS Dashboard Backend..."
echo "📁 Working directory: $(pwd)"
echo ""

# Kill any existing process on port 3000
if lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "⚠️  Port 3000 is in use, killing existing process..."
  lsof -ti:3000 | xargs kill -9 2>/dev/null || true
  sleep 1
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
  echo "📦 Installing dependencies..."
  npm install
fi

echo ""
echo "✅ Backend is starting on http://localhost:3000"
echo "🛑 Press Ctrl+C to stop"
echo ""

# Run in foreground
npm run start:dev
