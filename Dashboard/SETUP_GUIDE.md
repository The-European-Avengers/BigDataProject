# HDFS Dashboard - Local Development Setup Guide

## Quick Start

Simply run the startup script from the Dashboard directory:

```bash
cd Dashboard
./start.sh
```

The script will:
- ✓ Check for Node.js and npm installation
- ✓ Verify directory structure
- ✓ Install/verify dependencies
- ✓ Start the backend server (port 3000)
- ✓ Start the frontend development server

## Requirements

### Essential
- **Node.js** (v14 or higher) - Download from https://nodejs.org/
- **npm** (comes with Node.js)
- **Ports available**: 3000 (backend API), and an available port for the frontend dev server (typically 3001)

### Optional (for full functionality)
- **Redis** (for caching) - http://localhost:6379
- **Hive/HiveServer2** (for database queries) - http://localhost:10000

> **Note**: The application will still start and run without Redis and Hive, but caching and database queries won't work. This is fine for development UI testing.

## System Requirements

### macOS
```bash
# Using Homebrew
brew install node
```

### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install nodejs npm
```

### Windows
Download from https://nodejs.org/ and run the installer

### Verify Installation
```bash
node --version
npm --version
```

## What the Script Does

1. **Checks Prerequisites**
   - Verifies Node.js and npm are installed
   - Confirms backend and frontend directories exist
   - Checks for required ports availability

2. **Installs Dependencies**
   - Installs backend npm packages (with legacy peer deps flag for compatibility)
   - Installs frontend npm packages
   - Uses cached installations if dependencies already exist

3. **Starts Backend Server**
   - Runs on port 3000
   - NestJS development server with auto-reload
   - API endpoints available at `http://localhost:3000`
   - Swagger API documentation at `http://localhost:3000/api/docs`

4. **Starts Frontend Server**
   - React development server
   - Hot module reloading enabled
   - Opens browser automatically (if configured)
   - Proxies API calls to backend

## Running the Script

### From Command Line

```bash
# Navigate to Dashboard directory
cd /path/to/BigDataProject/Dashboard

# Run the startup script
./start.sh
```

### Output
```
═══════════════════════════════════════════════════════════
    HDFS Dashboard - Local Development Server Startup
═══════════════════════════════════════════════════════════

✓ Node.js and npm found
✓ Backend and frontend directories found

Checking dependencies...
✓ Backend dependencies already installed
✓ Frontend dependencies already installed

═══════════════════════════════════════════════════════════
Starting servers...
═══════════════════════════════════════════════════════════

Starting Backend Server...
✓ Backend Server started (PID: 12345)
  📍 Backend API: http://localhost:3000
  📚 API Docs: http://localhost:3000/api/docs

Starting Frontend Server...
✓ Frontend Server started
  🌐 Frontend: http://localhost:3000 (React dev server on different port)
```

## Accessing the Application

Once both servers are running:

- **Frontend Dashboard**: The React dev server will typically run on `http://localhost:3001` or print the URL in the terminal
- **Backend API**: `http://localhost:3000`
- **API Documentation**: `http://localhost:3000/api/docs`

## Stopping the Servers

Simply press **Ctrl+C** in the terminal running the script. It will gracefully shut down both servers.

## Troubleshooting

### Port Already in Use
If you get an error about ports being in use:

```bash
# Kill process on port 3000 (backend)
lsof -ti:3000 | xargs kill -9

# Kill process on port 3001 (frontend, or whatever port is shown)
lsof -ti:3001 | xargs kill -9
```

### Dependencies Installation Fails
If npm install fails:

```bash
# Clear npm cache
npm cache clean --force

# Remove node_modules and lock files
rm -rf backend/node_modules backend/package-lock.json
rm -rf frontend/node_modules frontend/package-lock.json

# Try running the script again
./start.sh
```

### Backend Warnings About Redis/Hive
These warnings are expected if you don't have Redis and Hive running locally:
```
⚠️ Redis connection failed (will continue without caching)
⚠️ Could not connect to Hive on init
```
This is normal for local development - the app will continue working without caching and DB functionality.

### Frontend Won't Start
Check that the backend is running first:
```bash
curl http://localhost:3000/health
```

Should return a success response. If not, check the backend logs.

## Optional: Setting Up Redis and Hive

If you want full functionality with caching and database queries:

### Redis Setup (macOS with Homebrew)
```bash
brew install redis
brew services start redis
redis-cli ping  # Should return PONG
```

### Redis Setup (Docker)
```bash
docker run -d -p 6379:6379 redis:latest
```

### Hive Setup
Hive setup is more complex and typically requires a full Hadoop cluster. For development, you may use Docker Compose:

```bash
cd Dashboard
docker-compose up -d
```

## Backend Configuration

Edit `backend/.env` to configure backend settings:

```env
# Server
API_PORT=3000
NODE_ENV=development

# Redis (optional)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Hive Database (optional)
HIVE_HOST=localhost
HIVE_PORT=10000
HIVE_DATABASE=analytics
HIVE_USER=
HIVE_PASSWORD=

# Cache
CACHE_TTL=3600
```

## Frontend Configuration

The frontend automatically proxies to the backend. To change the API URL:

Edit `frontend/src/services/api.js`:

```javascript
const api = axios.create({
  baseURL: process.env.REACT_APP_API_URL || 'http://localhost:3000'
});
```

Or set the environment variable:
```bash
export REACT_APP_API_URL=http://your-api-url:3000
npm start
```

## Development Workflow

1. **Run the startup script** once to start both servers
2. **Edit code** - Both servers have hot-reload enabled
3. **Frontend changes** - Browser auto-refreshes
4. **Backend changes** - Server auto-restarts
5. **Commit changes** to git when satisfied

## Additional Scripts

### Manual Backend Start
```bash
cd backend
npm run start:dev
```

### Manual Frontend Start
```bash
cd frontend
npm start
```

### Backend Build
```bash
cd backend
npm run build
npm run start:prod
```

### Frontend Build
```bash
cd frontend
npm run build
```

## Next Steps

- 📖 Read the main [README.md](./README.md) for project overview
- 🐳 Check [DOCKER_DEPLOYMENT.md](./DOCKER_DEPLOYMENT.md) for Docker setup
- ☸️ See the `k8s/` directory for Kubernetes deployment configs
- 📚 View Swagger API docs at `http://localhost:3000/api/docs`

---

**Need Help?** Check the logs:
- Backend logs: `/tmp/hdfs-backend.log`
- Frontend logs: Terminal output
