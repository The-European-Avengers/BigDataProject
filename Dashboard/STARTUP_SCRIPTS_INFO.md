# Startup Scripts - Summary

Two startup scripts have been created to easily start the HDFS Dashboard locally.

## Files Created

### 1. `start.sh` (macOS/Linux)
- **Location**: `/Dashboard/start.sh`
- **Usage**: `./start.sh`
- **Features**:
  - Checks Node.js/npm installation
  - Auto-installs dependencies if needed
  - Validates directory structure
  - Starts backend in background
  - Starts frontend in foreground
  - Graceful shutdown on Ctrl+C
  - Color-coded output for easy reading
  - Logs to `/tmp/hdfs-backend.log`

### 2. `start.bat` (Windows)
- **Location**: `/Dashboard/start.bat`
- **Usage**: Double-click or run `start.bat` in Command Prompt
- **Features**:
  - Checks Node.js/npm installation
  - Auto-installs dependencies if needed
  - Starts backend in separate window
  - Starts frontend in current window
  - Clear error messages

### 3. `SETUP_GUIDE.md` (Comprehensive)
- Detailed system requirements
- Step-by-step installation guide
- Troubleshooting section
- Optional service setup (Redis, Hive)
- Configuration options

### 4. `QUICK_START_GUIDE.md` (TL;DR)
- Quick reference guide
- One-command startup
- Common problems & solutions
- Expected output

---

## What Needs to Be Running

### Essential ✅ (Must Have)
- **Node.js** v14 or higher
- **npm** (comes with Node.js)
- **Ports 3000+** available (backend: 3000, frontend: 3001+)

### Required to Start Scripts ✅
- Node.js installed and in system PATH
- `start.sh` (Mac/Linux) must have execute permission (automatically set)
- Backend directory at `./backend`
- Frontend directory at `./frontend`

### Optional for Full Features ⚠️ (Nice to Have)
- **Redis** (localhost:6379) - For caching functionality
- **Hive** (localhost:10000) - For database queries

### What Happens if Optional Services Aren't Available
- Backend will show warnings but still start
- Backend API will work without caching
- Frontend will work but won't show live database data
- **This is fine for UI/development testing**

---

## System Setup Checklist

Before running the script, ensure:

- [ ] Node.js installed (`node --version` returns v14+)
- [ ] npm installed (`npm --version` returns a version)
- [ ] You're in the `Dashboard` directory
- [ ] The script is executable (Mac/Linux: `chmod +x start.sh`)
- [ ] Ports 3000 and 3001+ are available

---

## Quick Start

### macOS/Linux
```bash
cd /path/to/BigDataProject/Dashboard
./start.sh
```

### Windows
```bash
cd \path\to\BigDataProject\Dashboard
start.bat
```

Or double-click `start.bat` in File Explorer.

---

## Expected Behavior

1. Script checks prerequisites (Node.js, directories)
2. Script installs dependencies if needed (first run only)
3. Backend server starts on port 3000
4. Frontend dev server starts (opens in browser or shows URL)
5. Both servers are ready for use

Typical startup time: **30-60 seconds** (first run takes longer due to dependency installation)

---

## Stopping the Servers

Press **Ctrl+C** in the terminal/command prompt. The script will:
1. Gracefully shut down both servers
2. Clean up background processes
3. Exit cleanly

---

## Files Generated During Runtime

- **Backend logs**: `/tmp/hdfs-backend.log` (Mac/Linux)
- **Node modules**: `backend/node_modules/`, `frontend/node_modules/`
- **Lock files**: `backend/package-lock.json`, `frontend/package-lock.json`

---

## Next Steps

1. Run the startup script
2. Open the dashboard URL (typically http://localhost:3001)
3. Development changes will hot-reload automatically
4. See [SETUP_GUIDE.md](./SETUP_GUIDE.md) for advanced configuration

---

## Support

### For Detailed Help
Read [SETUP_GUIDE.md](./SETUP_GUIDE.md) which includes:
- Troubleshooting section
- Optional service setup
- Configuration options
- Development workflow

### Common Issues
- **Port in use**: Kill process with `lsof -ti:3000 | xargs kill -9` (Mac/Linux)
- **Dependencies fail**: Delete `node_modules` folders and run script again
- **Script won't run**: Make sure Node.js is installed and `start.sh` is executable

---

**Created**: December 21, 2025
**For**: HDFS Dashboard - Local Development
