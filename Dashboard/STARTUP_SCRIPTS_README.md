# HDFS Dashboard - Startup Scripts Documentation

This directory contains everything needed to run the HDFS Dashboard backend and frontend locally.

## 📋 Files Included

| File | Purpose | Platform |
|------|---------|----------|
| **start.sh** | Main startup script | macOS/Linux ✅ |
| **start.bat** | Main startup script | Windows ✅ |
| **check-environment.sh** | Environment verification | macOS/Linux ✅ |
| **QUICK_START_GUIDE.md** | Quick reference | All 📄 |
| **SETUP_GUIDE.md** | Comprehensive guide | All 📄 |
| **STARTUP_SCRIPTS_INFO.md** | Script details | All 📄 |

## 🚀 Quick Start (Choose Your OS)

### macOS/Linux
```bash
cd /path/to/Dashboard
./start.sh
```

### Windows
```bash
cd C:\path\to\Dashboard
start.bat
```

Or simply **double-click** `start.bat` in File Explorer.

## ✅ Requirements

### Essential
- **Node.js v14+** - Download from https://nodejs.org/
- **npm** - Comes with Node.js
- **Available Ports**: 3000 (backend), 3001+ (frontend)

### Optional (for full functionality)
- **Redis** - For caching functionality
- **Hive** - For database queries

> **Note**: The application will run without Redis and Hive, but some features won't work.

## 🔍 Check Your Environment

Before running the startup script, verify your system is ready:

```bash
./check-environment.sh
```

This will check for:
- ✓ Node.js and npm installation
- ✓ Backend and frontend directories
- ✓ Port availability
- ✓ Optional services (Redis, Hive)

## 📖 Documentation

Read these files for detailed information:

1. **[QUICK_START_GUIDE.md](./QUICK_START_GUIDE.md)** - For the impatient
   - One-command startup
   - Common issues & fixes
   - Expected output

2. **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** - Comprehensive guide
   - Detailed requirements
   - Manual setup steps
   - Troubleshooting
   - Optional service setup (Redis, Hive)
   - Configuration options

3. **[STARTUP_SCRIPTS_INFO.md](./STARTUP_SCRIPTS_INFO.md)** - What runs
   - Script features
   - What needs to be running
   - System setup checklist
   - Common issues table

## 🎯 What the Scripts Do

### Automatically
1. ✓ Check Node.js and npm installation
2. ✓ Verify directory structure
3. ✓ Install dependencies (only if needed)
4. ✓ Start backend server (port 3000)
5. ✓ Start frontend dev server
6. ✓ Provide clear output with URLs

### You Don't Need to Do
- ✗ Install dependencies manually
- ✗ Create configuration files
- ✗ Start services separately
- ✗ Copy environment files

## 🌐 Accessing the Dashboard

Once the script completes:

- **Frontend Dashboard**: Usually http://localhost:3001 (check terminal output)
- **Backend API**: http://localhost:3000
- **API Documentation**: http://localhost:3000/api/docs

## ⛔ Stopping the Servers

Simply press **Ctrl+C** in the terminal. The script will gracefully shut down both servers.

## ⚠️ Common Warnings (Don't Panic!)

You might see these on startup:
```
⚠️ Redis connection failed (will continue without caching)
⚠️ Could not connect to Hive on init
```

**This is normal!** The app still works perfectly. These warnings only appear if:
- Redis isn't running (optional service)
- Hive isn't available (optional service)

The application will continue to work without these services.

## 🐛 Troubleshooting

### Port Already in Use
```bash
# Kill process on port 3000
lsof -ti:3000 | xargs kill -9

# Then run the script again
./start.sh
```

### Dependencies Won't Install
```bash
# Remove node_modules and lock files
rm -rf backend/node_modules frontend/node_modules
rm -rf backend/package-lock.json frontend/package-lock.json

# Clear npm cache
npm cache clean --force

# Run the script again
./start.sh
```

### Script Won't Execute (Mac/Linux)
```bash
# Make it executable
chmod +x start.sh

# Then run it
./start.sh
```

### Node.js Not Found
Install Node.js from https://nodejs.org/ and make sure it's in your system PATH.

For more troubleshooting, see **[SETUP_GUIDE.md](./SETUP_GUIDE.md)**.

## 🔧 Manual Startup (If Needed)

If the script doesn't work, you can start servers manually:

### Backend
```bash
cd backend
npm install --legacy-peer-deps
npm run start:dev
```

### Frontend (in another terminal)
```bash
cd frontend
npm install
npm start
```

## 📚 Backend Configuration

Edit `backend/.env` to customize:
```env
API_PORT=3000
NODE_ENV=development
REDIS_HOST=localhost
HIVE_HOST=localhost
HIVE_DATABASE=analytics
```

## 🎨 Frontend Configuration

The frontend proxies to the backend automatically. To use a different API URL:

```bash
REACT_APP_API_URL=http://your-server:3000 npm start
```

## 🐳 Docker Alternative

If you prefer Docker:

```bash
docker-compose up
```

This starts both services in containers (requires Docker installed).

## 📱 Development Features

Both servers have hot-reload enabled:
- **Backend**: Changes trigger automatic restart
- **Frontend**: Changes trigger browser refresh

## 🆘 Need Help?

1. Read **[QUICK_START_GUIDE.md](./QUICK_START_GUIDE.md)** for quick answers
2. Check **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** for detailed help
3. Run `./check-environment.sh` to diagnose issues
4. Check `/tmp/hdfs-backend.log` for backend errors (Mac/Linux)
5. Look at terminal output for frontend errors

## 📝 System Requirements Summary

| Requirement | Min Version | Status |
|-------------|-------------|--------|
| Node.js | 14.0.0 | Required ✅ |
| npm | 6.0.0 | Required ✅ |
| macOS/Linux/Windows | Any | Required ✅ |
| Redis | 5.0.0 | Optional ⚠️ |
| Hive | 2.0.0 | Optional ⚠️ |

## 🎉 Success Checklist

- [ ] Node.js v14+ installed (`node --version`)
- [ ] npm installed (`npm --version`)
- [ ] Ports 3000+ available
- [ ] Run `./check-environment.sh` and see "Your system is ready!"
- [ ] Run `./start.sh` (or `start.bat` on Windows)
- [ ] See "Backend Server started" message
- [ ] Open the URL shown in the terminal
- [ ] Dashboard loads successfully

## 📞 Support Resources

- **Node.js Help**: https://nodejs.org/
- **React Developer Guide**: https://react.dev/
- **NestJS Documentation**: https://docs.nestjs.com/
- **Recharts (Charts Library)**: https://recharts.org/

---

**Ready to start?** Run `./start.sh` (or `start.bat` on Windows) and you're done! 🚀

For detailed information, see the included documentation files.
