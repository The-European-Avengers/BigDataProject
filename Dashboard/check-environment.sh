#!/bin/bash

# HDFS Dashboard - Environment Checker
# This script verifies that your system is ready to run the startup scripts

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  HDFS Dashboard - Environment Checker${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

CHECKS_PASSED=0
CHECKS_TOTAL=0

# Function to check command
check_command() {
    local cmd=$1
    local name=$2
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    
    if command -v $cmd &> /dev/null; then
        local version=$($cmd --version 2>/dev/null | head -n1)
        echo -e "${GREEN}✓${NC} $name installed"
        echo "  └─ $version"
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
        return 0
    else
        echo -e "${RED}✗${NC} $name NOT installed"
        echo "  └─ Required for the startup script to work"
        return 1
    fi
}

# Function to check file/directory
check_file() {
    local path=$1
    local name=$2
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    
    if [ -e "$path" ]; then
        echo -e "${GREEN}✓${NC} $name found"
        echo "  └─ $path"
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
        return 0
    else
        echo -e "${RED}✗${NC} $name NOT found"
        echo "  └─ Expected at: $path"
        return 1
    fi
}

# Function to check port availability
check_port() {
    local port=$1
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠${NC} Port $port is in use"
        local pid=$(lsof -t -i :$port 2>/dev/null | head -n1)
        if [ ! -z "$pid" ]; then
            echo "  └─ PID: $pid (kill with: kill -9 $pid)"
        fi
    else
        echo -e "${GREEN}✓${NC} Port $port is available"
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
    fi
}

echo -e "${BLUE}1. Required Software${NC}"
echo "───────────────────────────────────────────────────────────"
check_command "node" "Node.js"
echo ""
check_command "npm" "npm"
echo ""

echo -e "${BLUE}2. Dashboard Files${NC}"
echo "───────────────────────────────────────────────────────────"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
check_file "$SCRIPT_DIR/backend" "Backend directory"
echo ""
check_file "$SCRIPT_DIR/frontend" "Frontend directory"
echo ""
check_file "$SCRIPT_DIR/start.sh" "Startup script (start.sh)"
echo ""

echo -e "${BLUE}3. Port Availability${NC}"
echo "───────────────────────────────────────────────────────────"
check_port 3000
echo ""
check_port 3001
echo ""

echo -e "${BLUE}4. Optional Services${NC}"
echo "───────────────────────────────────────────────────────────"

CHECKS_TOTAL=$((CHECKS_TOTAL + 2))

if command -v redis-cli &> /dev/null; then
    if redis-cli ping &> /dev/null; then
        echo -e "${GREEN}✓${NC} Redis is running"
        echo "  └─ Port: 6379 (optional, for caching)"
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
    else
        echo -e "${YELLOW}⚠${NC} Redis not running"
        echo "  └─ Optional. Install with: brew install redis"
    fi
else
    echo -e "${YELLOW}⚠${NC} Redis not installed"
    echo "  └─ Optional. Install with: brew install redis"
fi
echo ""

if nc -z localhost 10000 &> /dev/null; then
    echo -e "${GREEN}✓${NC} Hive is available"
    echo "  └─ Port: 10000 (optional, for database queries)"
    CHECKS_PASSED=$((CHECKS_PASSED + 1))
else
    echo -e "${YELLOW}⚠${NC} Hive not available"
    echo "  └─ Optional. Setup required for database queries"
fi
echo ""

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Summary${NC}"
echo "───────────────────────────────────────────────────────────"

if [ $CHECKS_PASSED -ge 5 ]; then
    echo -e "${GREEN}✓ Your system is ready to run the startup scripts!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Run: ./start.sh"
    echo "2. Open: http://localhost:3001 (or shown in output)"
    echo "3. Enjoy the HDFS Dashboard!"
else
    echo -e "${YELLOW}⚠ Some checks failed. Please review above.${NC}"
    echo ""
    echo "Before running the startup script, you need:"
    echo "  • Node.js v14+ (https://nodejs.org/)"
    echo "  • npm (comes with Node.js)"
    echo ""
    echo "Optional but recommended:"
    echo "  • Redis for caching (brew install redis)"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo ""
