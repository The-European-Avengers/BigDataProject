#!/bin/bash

# ============================================================================
# Automated Hive Data Transfer to HDFS Script
# ============================================================================

set -e  # Exit on any error

# Configuration
NAMESPACE="bd-bd-gr-05"
HIVE_DEPLOYMENT="hive-server"
HIVE_CONTAINER="hive"
SHARED_DATA_PATH="/shared-data-for-hive"
CSV_FILE="2020_dmi_wind.csv"
DATABASE_NAME="dmi_wind"
TABLE_NAME="wind_raw_data"
HDFS_LOCATION="hdfs://namenode-g5:9000/raw/initial/weather-wind"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if CSV file exists in the shared volume
check_csv_exists() {
    log_info "Checking if CSV file exists in shared volume..."
    
    local csv_exists=$(kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        test -f "$SHARED_DATA_PATH/$CSV_FILE" && echo "yes" || echo "no")
    
    if [ "$csv_exists" == "yes" ]; then
        log_info "✓ CSV file found: $SHARED_DATA_PATH/$CSV_FILE"
        return 0
    else
        log_error "✗ CSV file not found: $SHARED_DATA_PATH/$CSV_FILE"
        return 1
    fi
}

# Execute Hive command
execute_hive_command() {
    local hive_cmd=$1
    
    kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        hive -e "$hive_cmd"
}

# ============================================================================
# Main Process
# ============================================================================

main() {
    log_info "============================================"
    log_info "Automated HDFS Data Transfer"
    log_info "============================================"
    echo ""
    
    # Verify pods are running
    log_info "Step 1: Verifying Hive deployment is running..."
    local pod_status=$(kubectl get pods -n $NAMESPACE -l app=$HIVE_DEPLOYMENT -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "NotFound")
    
    if [ "$pod_status" != "Running" ]; then
        log_error "Hive deployment is not running (status: $pod_status)"
        log_error "Please ensure your deployment is running before transferring data"
        exit 1
    fi
    log_info "✓ Hive deployment is running"
    echo ""
    
    # Check CSV exists
    log_info "Step 2: Verifying CSV file exists..."
    if ! check_csv_exists; then
        log_error "Cannot proceed without CSV file"
        log_info "Checking collector container logs..."
        kubectl logs -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c wind-collector --tail=30
        exit 1
    fi
    
    # Show file details
    log_info "CSV file details:"
    kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        ls -lh "$SHARED_DATA_PATH/$CSV_FILE"
    echo ""
    
    # Test Hive access
    log_info "Step 3: Verifying Hive shell access..."
    if execute_hive_command "SHOW DATABASES;" > /dev/null 2>&1; then
        log_info "✓ Hive shell is accessible"
    else
        log_error "Cannot access Hive shell"
        exit 1
    fi
    echo ""
    
    # Create database and table
    log_info "Step 4: Creating Hive database and table..."
    
    local create_sql="
    CREATE DATABASE IF NOT EXISTS ${DATABASE_NAME};
    USE ${DATABASE_NAME};
    
    CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE_NAME}.${TABLE_NAME} (
      timeObserved STRING,
      stationId STRING,
      stationName STRING,
      mean_wind_speed DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '${HDFS_LOCATION}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    "
    
    execute_hive_command "$create_sql"
    log_info "✓ Database '${DATABASE_NAME}' and table '${TABLE_NAME}' created"
    echo ""
    
    # Load CSV into HDFS
    log_info "Step 5: Loading CSV data into HDFS..."
    
    # First check if file is accessible from Hive's perspective
    log_info "Verifying file accessibility from Hive container..."
    kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        ls -la "$SHARED_DATA_PATH/" || {
            log_error "Cannot access shared data path from Hive container"
            log_info "Listing root shared path:"
            kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- ls -la /shared-data-for-hive/ || true
            exit 1
        }
    
    local load_sql="
    USE ${DATABASE_NAME};
    LOAD DATA LOCAL INPATH '${SHARED_DATA_PATH}/${CSV_FILE}' INTO TABLE ${DATABASE_NAME}.${TABLE_NAME};
    "
    
    if execute_hive_command "$load_sql"; then
        log_info "✓ Data loaded into HDFS"
    else
        log_error "Failed to load data. Checking if file still exists..."
        kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
            ls -la "$SHARED_DATA_PATH/" || true
        exit 1
    fi
    echo ""
    
    # Verify data count
    log_info "Step 6: Verifying data was loaded..."
    echo ""
    log_info "Row count:"
    execute_hive_command "USE ${DATABASE_NAME}; SELECT count(*) FROM ${TABLE_NAME};"
    echo ""
    
    log_info "Sample data (first 10 rows):"
    execute_hive_command "USE ${DATABASE_NAME}; SELECT * FROM ${TABLE_NAME} LIMIT 10;"
    echo ""
    
    # Verify file in HDFS
    log_info "Step 7: Verifying CSV file in HDFS..."
    kubectl exec -n $NAMESPACE deployment/$HIVE_DEPLOYMENT -c $HIVE_CONTAINER -- \
        hdfs dfs -ls $HDFS_LOCATION
    echo ""
    
    log_info "============================================"
    log_info "✓ Data transfer completed successfully!"
    log_info "Your CSV data is now in HDFS and ready for Spark processing"
    log_info "============================================"
}

# Run main function
main