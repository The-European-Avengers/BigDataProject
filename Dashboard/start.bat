@echo off
REM HDFS Dashboard - Windows Startup Script

setlocal enabledelayedexpansion

echo.
echo ===============================================================
echo     HDFS Dashboard - Local Development Server Startup (Windows)
echo ===============================================================
echo.

REM Check if Node.js is installed
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Node.js is not installed or not in PATH
    echo Please install Node.js from https://nodejs.org/
    pause
    exit /b 1
)

REM Check if npm is installed
where npm >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] npm is not installed or not in PATH
    echo Please install npm ^(comes with Node.js^)
    pause
    exit /b 1
)

echo [OK] Node.js and npm found
echo.

REM Get the directory where this script is located
set SCRIPT_DIR=%~dp0

REM Check if backend directory exists
if not exist "%SCRIPT_DIR%backend" (
    echo [ERROR] Backend directory not found at %SCRIPT_DIR%backend
    pause
    exit /b 1
)

REM Check if frontend directory exists
if not exist "%SCRIPT_DIR%frontend" (
    echo [ERROR] Frontend directory not found at %SCRIPT_DIR%frontend
    pause
    exit /b 1
)

echo [OK] Backend and frontend directories found
echo.

REM Check and install dependencies
echo Checking dependencies...
echo.

if not exist "%SCRIPT_DIR%backend\node_modules" (
    echo Installing backend dependencies...
    cd /d "%SCRIPT_DIR%backend"
    call npm install --legacy-peer-deps --silent
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to install backend dependencies
        pause
        exit /b 1
    )
    echo [OK] Backend dependencies installed
) else (
    echo [OK] Backend dependencies already installed
)

if not exist "%SCRIPT_DIR%frontend\node_modules" (
    echo Installing frontend dependencies...
    cd /d "%SCRIPT_DIR%frontend"
    call npm install --silent
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to install frontend dependencies
        pause
        exit /b 1
    )
    echo [OK] Frontend dependencies installed
) else (
    echo [OK] Frontend dependencies already installed
)

echo.
echo ===============================================================
echo Starting servers...
echo ===============================================================
echo.

REM Start backend server in a new window
echo Starting Backend Server...
cd /d "%SCRIPT_DIR%backend"
start "HDFS Backend Server" cmd /k npm run start:dev
timeout /t 3 /nobreak

echo [OK] Backend Server started
echo      API: http://localhost:3000
echo      Docs: http://localhost:3000/api/docs
echo.

REM Start frontend server in current window
echo Starting Frontend Server...
cd /d "%SCRIPT_DIR%frontend"
echo [OK] Frontend Server starting
echo      Dashboard will open at http://localhost:3001 ^(or shown in output^)
echo.
echo Press Ctrl+C in the Backend window to stop the backend
echo Press Ctrl+C here to stop the frontend
echo.

call npm start

pause
