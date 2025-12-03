#!/bin/bash
# entrypoint-batch.sh - Entrypoint for batch enrichment job

set -e

echo "============================================================"
echo "  BATCH ENRICHMENT JOB - STARTING"
echo "============================================================"
echo ""
echo "Environment:"
echo "  HDFS_NAMENODE: ${HDFS_NAMENODE}"
echo "  MUNICIPALITY_CSV: ${MUNICIPALITY_CSV}"
echo "  USER: $(whoami)"
echo "  WORKING DIR: $(pwd)"
echo ""

echo "Checking files..."
echo "  batch_enrichment.py: $(ls -lh batch_enrichment.py 2>/dev/null || echo 'NOT FOUND')"
echo "  Municipality CSV: $(ls -lh ${MUNICIPALITY_CSV} 2>/dev/null || echo 'NOT FOUND')"
echo ""

echo "Starting Python script..."
echo ""

# Run the batch enrichment job
exec python3 batch_enrichment.py