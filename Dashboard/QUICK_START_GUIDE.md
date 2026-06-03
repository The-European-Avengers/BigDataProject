# Quick Start Guide

## TL;DR - Just Want to Run It?

### macOS/Linux:
```bash
cd Dashboard
./start.sh
```

### Windows:
```bash
cd Dashboard
start.bat
```

That's it! The script handles everything else.

---

## What You Need Before Running

### ✅ Required
- **Node.js v14+** - Get it from https://nodejs.org/
- **Free Ports**: 3000, 3001 (or whatever React picks)

### ⚠️ Optional (for full functionality)
- **Redis** - If you want caching. Otherwise it works without it.
- **Hive** - If you want database access. Otherwise it works without it.

### ✓ Already Installed?
Check with:
```bash
node --version
npm --version
```

---

## Expected Output

When you run the script, you'll see:
```
✓ Backend Server started (PID: XXXXX)
  📍 Backend API: http://localhost:3000
  📚 API Docs: http://localhost:3000/api/docs

✓ Frontend Server started
  🌐 Frontend: http://localhost:3000 (React dev server on different port)
```

Open the URL shown in your terminal to see the dashboard.

---

## Common Warnings (Don't Panic!)

**See this?**
```
⚠️ Redis connection failed (will continue without caching)
⚠️ Could not connect to Hive on init
```

This is **normal**! The app still works, just without caching and database queries.

---

## Stopping the Servers

Press **Ctrl+C** in the terminal. Both servers will shut down automatically.

---

## If Something Goes Wrong

| Problem | Solution |
|---------|----------|
| Port 3000 in use | `lsof -ti:3000 \| xargs kill -9` (Mac/Linux) |
| Dependencies fail | Delete `backend/node_modules` and `frontend/node_modules`, run script again |
| Node.js not found | Install from https://nodejs.org/ |
| Script won't run (Mac/Linux) | Run `chmod +x start.sh` first |

---

## Want More Details?

Read [SETUP_GUIDE.md](./SETUP_GUIDE.md) for:
- Full system requirements
- Backend/frontend configuration options
- Setting up Redis and Hive
- Development workflow
- Troubleshooting guide

---

**Happy developing! 🚀**
