#!/bin/bash

# Simple restart script - kills and restarts servers

echo "Stopping old servers..."
pkill -f "npm run start:dev" 2>/dev/null || true
pkill -f "npm start" 2>/dev/null || true
sleep 2

echo "Starting backend..."
cd /Users/alknific/Documents/FAKS/BigDataProject/Dashboard/backend
npm run start:dev > /tmp/backend.log 2>&1 &
BACKEND_PID=$!
echo "Backend PID: $BACKEND_PID"

sleep 5

echo "Starting frontend..."
cd /Users/alknific/Documents/FAKS/BigDataProject/Dashboard/frontend
npm start > /tmp/frontend.log 2>&1 &
FRONTEND_PID=$!
echo "Frontend PID: $FRONTEND_PID"

sleep 3

echo ""
echo "Checking servers..."
curl -s http://localhost:3000/health > /dev/null && echo "✓ Backend running on http://localhost:3000" || echo "✗ Backend not responding"

echo ""
echo "Frontend should be on http://localhost:3001"
echo ""
echo "Logs:"
echo "  Backend:  tail -f /tmp/backend.log"
echo "  Frontend: tail -f /tmp/frontend.log"
echo ""
echo "To stop servers:"
echo "  pkill -f 'npm run start:dev'"
echo "  pkill -f 'npm start'"
