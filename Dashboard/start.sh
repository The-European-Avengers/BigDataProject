#!/bin/bash

# HDFS Dashboard - Startup Script
# Simple and reliable way to start frontend and backend

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║   HDFS Dashboard - Starting Backend & Frontend             ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down servers..."
    # Kill all background processes
    jobs -p 2>/dev/null | xargs -r kill 2>/dev/null
    echo "✓ Servers stopped"
    exit 0
}

# Trap Ctrl+C to cleanup
trap cleanup SIGINT SIGTERM EXIT

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js is not installed"
    echo "Please install from: https://nodejs.org/"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "ERROR: npm is not installed"
    exit 1
fi

echo "✓ Node.js and npm found"
echo ""

# Verify directories
if [ ! -d "$SCRIPT_DIR/backend" ]; then
    echo "ERROR: Backend directory not found"
    exit 1
fi

if [ ! -d "$SCRIPT_DIR/frontend" ]; then
    echo "ERROR: Frontend directory not found"
    exit 1
fi

echo "✓ Backend and frontend directories found"
echo ""

# Check and install dependencies
echo "Checking dependencies..."

if [ ! -d "$SCRIPT_DIR/backend/node_modules" ]; then
    echo "Installing backend dependencies..."
    cd "$SCRIPT_DIR/backend"
    npm install --legacy-peer-deps --silent 2>&1 | grep -E "added|packages|error" || true
    echo "✓ Backend dependencies installed"
else
    echo "✓ Backend dependencies already installed"
fi

if [ ! -d "$SCRIPT_DIR/frontend/node_modules" ]; then
    echo "Installing frontend dependencies..."
    cd "$SCRIPT_DIR/frontend"
    npm install --silent 2>&1 | grep -E "added|packages|error" || true
    echo "✓ Frontend dependencies installed"
else
    echo "✓ Frontend dependencies already installed"
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "Starting servers..."
echo "════════════════════════════════════════════════════════════"
echo ""

# Kill any existing servers on these ports
echo "Cleaning up old processes..."
pkill -f "npm run start:dev" 2>/dev/null || true
pkill -f "npm start" 2>/dev/null || true
sleep 2

# Start backend server in background
echo "Starting Backend Server..."
cd "$SCRIPT_DIR/backend"
npm run start:dev > /tmp/hdfs-backend.log 2>&1 &
BACKEND_PID=$!
sleep 4

# Check if backend is responding
if curl -s http://localhost:3000/health > /dev/null 2>&1; then
    echo "✓ Backend Server started (PID: $BACKEND_PID)"
    echo "  📍 Backend API: http://localhost:3000"
    echo "  📚 API Docs: http://localhost:3000/api/docs"
else
    echo "ERROR: Backend failed to start"
    echo "Log output:"
    cat /tmp/hdfs-backend.log
    exit 1
fi

echo ""

# Start frontend server
echo "Starting Frontend Server..."
cd "$SCRIPT_DIR/frontend"
npm start &
FRONTEND_PID=$!
sleep 3

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✓ Both servers are running!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Backend:  http://localhost:3000"
echo "Frontend: http://localhost:3001"
echo ""
echo "Press Ctrl+C to stop servers"
echo ""

# Wait for all background processes
wait
