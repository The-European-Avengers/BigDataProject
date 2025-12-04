#!/bin/bash

# ============================================================================
# Automated Hive Data Transfer - Electricity Price Data
# Handles: Day-ahead and Intraday Electricity Prices
# ============================================================================

set -e  # Exit on any error

# Configuration
NAMESPACE="bd-bd-gr-05"
HIVE_DEPLOYMENT="hive-server"
HIVE_CONTAINER="hive"
SHARED_DATA_PATH="/shared-data-for-hive"
NAMENODE="namenode-g5"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Execute Hive command
execute_hive_command() {
    local hive_cmd=$1
    
    kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        hive -e "$hive_cmd"
}

# List files in shared directory
list_shared_files() {
    kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        ls -1 "$SHARED_DATA_PATH/" 2>/dev/null || echo ""
}

# Load data files into table
load_files_to_table() {
    local database=$1
    local table=$2
    local file_pattern=$3
    
    local files=$(list_shared_files | grep "$file_pattern" || true)
    
    if [ -z "$files" ]; then
        log_warn "No files found matching pattern: $file_pattern"
        return 1
    fi
    
    local count=0
    while IFS= read -r file; do
        if [ -n "$file" ]; then
            log_info "  Loading: $file"
            local load_sql="USE ${database}; LOAD DATA LOCAL INPATH '${SHARED_DATA_PATH}/${file}' INTO TABLE ${table};"
            
            if execute_hive_command "$load_sql" 2>&1 | grep -q "OK" || true; then
                ((count++))
            else
                log_warn "  Failed to load: $file"
            fi
        fi
    done <<< "$files"
    
    log_info "  ✓ Loaded $count files"
    return 0
}

# ============================================================================
# Electricity Price Data Configuration
# ============================================================================

setup_electricity_price_data() {
    local database="price"
    local table="price_raw_data"
    local hdfs_location="hdfs://${NAMENODE}:9000/raw/initial-load/price"
    
    log_step "Setting up Electricity Price Data..."
    
    local create_sql="
    CREATE DATABASE IF NOT EXISTS ${database};
    USE ${database};
    
    CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
      mtu_utc STRING,
      area STRING,
      sequence STRING,
      day_ahead_price DOUBLE,
      intraday_period_utc STRING,
      intraday_price DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '${hdfs_location}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    "
    
    execute_hive_command "$create_sql"
    log_info "✓ Created database and table: ${database}.${table}"
    
    # Load all electricity price CSV files (pattern: DayAheadPrices_*.csv)
    log_info "Loading DayAheadPrices CSV files..."
    load_files_to_table "$database" "$table" "DayAheadPrices_"
    
    # Verify
    log_info "Verifying data count..."
    local count=$(execute_hive_command "USE ${database}; SELECT COUNT(*) FROM ${table};" | tail -1)
    log_info "✓ Total electricity price records: $count"
    echo ""
    
    # Show sample data
    log_info "Sample data (first 5 rows):"
    execute_hive_command "USE ${database}; SELECT * FROM ${table} LIMIT 5;" || true
    echo ""
}

# ============================================================================
# Main Process
# ============================================================================

main() {
    echo ""
    log_info "============================================"
    log_info "  Electricity Price Data Transfer to HDFS"
    log_info "============================================"
    echo ""
    
    # Verify pods are running
    log_step "Verifying Hive deployment is running..."
    local pod_status=$(kubectl get pods -n $NAMESPACE -l app=$HIVE_DEPLOYMENT -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "NotFound")
    
    if [ "$pod_status" != "Running" ]; then
        log_error "Hive deployment is not running (status: $pod_status)"
        exit 1
    fi
    log_info "✓ Hive deployment is running"
    echo ""
    
    # List available files
    log_step "Listing available CSV files in shared volume..."
    echo ""
    list_shared_files | while read -r file; do
        if [ -n "$file" ]; then
            echo "  - $file"
        fi
    done
    echo ""
    
    # Test Hive access
    log_step "Verifying Hive shell access..."
    if execute_hive_command "SHOW DATABASES;" > /dev/null 2>&1; then
        log_info "✓ Hive shell is accessible"
    else
        log_error "Cannot access Hive shell"
        exit 1
    fi
    echo ""
    
    # Setup electricity price data
    setup_electricity_price_data
    
    # Final summary
    log_info "============================================"
    log_info "  Summary - Price Database"
    log_info "============================================"
    echo ""
    
    log_info "Database created:"
    execute_hive_command "SHOW DATABASES;" | grep "price" || true
    echo ""
    
    log_info "Table created:"
    echo "  - price.price_raw_data"
    echo ""
    
    log_info "HDFS location:"
    echo "  - hdfs://${NAMENODE}:9000/raw/initial-load/price"
    echo ""
    
    log_info "Table schema:"
    echo "  - mtu_utc: Market Time Unit (timestamp range)"
    echo "  - area: Bidding zone (e.g., BZN|DK1)"
    echo "  - sequence: Sequence type"
    echo "  - day_ahead_price: Day-ahead price in EUR/MWh"
    echo "  - intraday_period_utc: Intraday period timestamp"
    echo "  - intraday_price: Intraday price in EUR/MWh"
    echo ""
    
    log_info "Query examples:"
    echo "  # Get average day-ahead price:"
    echo "  SELECT AVG(day_ahead_price) FROM price.price_raw_data WHERE day_ahead_price IS NOT NULL;"
    echo ""
    echo "  # Get prices for specific area:"
    echo "  SELECT * FROM price.price_raw_data WHERE area = 'BZN|DK1' LIMIT 10;"
    echo ""
    
    log_info "============================================"
    log_info "✓ Electricity price data transfer completed!"
    log_info "============================================"
}

# Run main function
main