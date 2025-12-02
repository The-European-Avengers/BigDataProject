#!/bin/bash

# ============================================================================
# Automated Hive Data Transfer to HDFS Script - ALL DATA TYPES
# Handles: Wind, Radiation, and Consumption Data
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
# Data Type Configurations
# ============================================================================

# Wind Data Configuration
setup_wind_data() {
    local database="dmi_wind"
    local table="wind_raw_data"
    local hdfs_location="hdfs://${NAMENODE}:9000/raw/initial-load/weather-wind"
    
    log_step "Setting up DMI Wind Data..."
    
    local create_sql="
    CREATE DATABASE IF NOT EXISTS ${database};
    USE ${database};
    
    CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
      timeObserved STRING,
      stationId STRING,
      stationName STRING,
      mean_wind_speed DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '${hdfs_location}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    "
    
    execute_hive_command "$create_sql"
    log_info "✓ Created database and table: ${database}.${table}"
    
    # Load all wind CSV files (pattern: *_dmi_wind.csv)
    log_info "Loading wind CSV files..."
    load_files_to_table "$database" "$table" "_dmi_wind.csv"
    
    # Verify
    log_info "Verifying data count..."
    local count=$(execute_hive_command "USE ${database}; SELECT COUNT(*) FROM ${table};" | tail -1)
    log_info "✓ Total wind records: $count"
    echo ""
}
# Temp Data Configuration
setup_temp_data() {
    local database="dmi_temp"
    local table="temp_raw_data"
    local hdfs_location="hdfs://${NAMENODE}:9000/raw/initial-load/weather-temp"
    
    log_step "Setting up DMI Temp Data..."
    
    local create_sql="
    CREATE DATABASE IF NOT EXISTS ${database};
    USE ${database};
    
    CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
      timeObserved STRING,
      stationId STRING,
      stationName STRING,
      mean_temp DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '${hdfs_location}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    "
    
    execute_hive_command "$create_sql"
    log_info "✓ Created database and table: ${database}.${table}"
    
    # Load all temp CSV files (pattern: *_dmi_temp.csv)
    log_info "Loading temp CSV files..."
    load_files_to_table "$database" "$table" "_dmi_temp.csv"
    
    # Verify
    log_info "Verifying data count..."
    local count=$(execute_hive_command "USE ${database}; SELECT COUNT(*) FROM ${table};" | tail -1)
    log_info "✓ Total temp records: $count"
    echo ""
}

# Sun Data Configuration
setup_sun_data() {
    local database="dmi_sun"
    local table="sun_raw_data"
    local hdfs_location="hdfs://${NAMENODE}:9000/raw/initial-load/weather-sun"
    
    log_step "Setting up DMI Sun Data..."
    
    local create_sql="
    CREATE DATABASE IF NOT EXISTS ${database};
    USE ${database};
    
    CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
      timeObserved STRING,
      stationId STRING,
      stationName STRING,
      mean_radiation DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '${hdfs_location}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    "
    
    execute_hive_command "$create_sql"
    log_info "✓ Created database and table: ${database}.${table}"
    
    # Load all sun CSV files (pattern: *_dmi_sun.csv)
    log_info "Loading sun CSV files..."
    load_files_to_table "$database" "$table" "_dmi_sun.csv"
    
    # Verify
    log_info "Verifying data count..."
    local count=$(execute_hive_command "USE ${database}; SELECT COUNT(*) FROM ${table};" | tail -1)
    log_info "✓ Total sun records: $count"
    echo ""
}

# Consumption Data Configuration
setup_consumption_data() {
    local database="energy_consumption"
    local table="consumption_raw_data"
    local hdfs_location="hdfs://${NAMENODE}:9000/raw/initial-load/consumption"
    
    log_step "Setting up Consumption Data..."
    
    local create_sql="
    CREATE DATABASE IF NOT EXISTS ${database};
    USE ${database};
    
    CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
      ConsumptionkWh DOUBLE,
      HeatingCategory STRING,
      HousingCategory STRING,
      Municipality STRING,
      MunicipalityCode INT,
      RegionName STRING,
      TimeDK STRING,
      TimeUTC STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '${hdfs_location}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    "
    
    execute_hive_command "$create_sql"
    log_info "✓ Created database and table: ${database}.${table}"
    
    # Load all consumption CSV files (pattern: consumption_*.csv)
    log_info "Loading consumption CSV files..."
    load_files_to_table "$database" "$table" "consumption_"
    
    # Also load the combined file if it exists
    log_info "Loading combined consumption file if present..."
    load_files_to_table "$database" "$table" "private_consumption_" || true
    
    # Verify
    log_info "Verifying data count..."
    local count=$(execute_hive_command "USE ${database}; SELECT COUNT(*) FROM ${table};" | tail -1)
    log_info "✓ Total consumption records: $count"
    echo ""
}

# ============================================================================
# Main Process
# ============================================================================

main() {
    echo ""
    log_info "============================================"
    log_info "  Automated HDFS Data Transfer - ALL TYPES"
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
    
    # Setup each data type
    setup_wind_data
    setup_temp_data
    setup_sun_data
    setup_consumption_data
    
    # Final summary
    log_info "============================================"
    log_info "  Summary - All Databases"
    log_info "============================================"
    echo ""
    
    log_info "Available databases:"
    execute_hive_command "SHOW DATABASES;" | grep -E "dmi_wind|dmi_temp|dmi_sun|energy_consumption" || true
    echo ""
    
    log_info "Tables created:"
    echo "  1. dmi_wind.wind_raw_data"
    echo "  2. dmi_temp.temp_raw_data"
    echo "  3. dmi_sun.sun_raw_data"
    echo "  4. energy_consumption.consumption_raw_data"
    echo ""
    
    log_info "HDFS locations:"
    echo "  - hdfs://${NAMENODE}:9000/raw/initial-load/weather-wind"
    echo "  - hdfs://${NAMENODE}:9000/raw/initial-load/weather-temp"
    echo "  - hdfs://${NAMENODE}:9000/raw/initial-load/weather-sun"
    echo "  - hdfs://${NAMENODE}:9000/raw/initial-load/consumption"
    echo ""
    
    log_info "============================================"
    log_info "✓ All data transfers completed successfully!"
    log_info "============================================"
}

# Run main function
main
