#!/bin/bash
# test_interactive.sh - Upload and run Spark test in interactive container

set -e

NAMESPACE="bd-bd-gr-05"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================================"
echo "  Spark Enrichment Test - Interactive Container"
echo "============================================================"
echo ""

# Find interactive pod
echo "Finding interactive pod..."
POD_NAME=$(kubectl get pods -n ${NAMESPACE} -l app=interactive --no-headers -o custom-columns=":metadata.name" 2>/dev/null | head -1)

if [ -z "$POD_NAME" ]; then
    echo -e "${RED}❌ Interactive pod not found${NC}"
    echo "Make sure the interactive container is running:"
    echo "  kubectl get pods -n ${NAMESPACE} | grep interactive"
    exit 1
fi

echo -e "${GREEN}✓ Found pod: ${POD_NAME}${NC}"
echo ""

# Check if test file exists locally
if [ ! -f "test_enrichment.py" ]; then
    echo -e "${RED}❌ test_enrichment.py not found in current directory${NC}"
    exit 1
fi

echo "Step 1: Uploading test script..."
kubectl cp test_enrichment.py ${POD_NAME}:/root/code/test_enrichment.py -n ${NAMESPACE}
echo -e "${GREEN}✓ Uploaded test_enrichment.py${NC}"
echo ""

echo "Step 2: Creating test data directory..."
kubectl exec ${POD_NAME} -n ${NAMESPACE} -- mkdir -p /root/code/data
echo -e "${GREEN}✓ Created /root/code/data${NC}"
echo ""

echo "Step 3: Creating sample municipality data..."
kubectl exec ${POD_NAME} -n ${NAMESPACE} -- bash -c 'cat > /root/code/data/municipality_codes_to_coordinates.csv << '\''EOF'\''
code,latitude,longitude
101,55.6761,12.5683
147,55.4038,10.4024
151,55.5364,9.3501
155,56.3286,9.1221
157,56.0997,8.4558
159,56.1496,10.2134
161,56.4697,9.4121
163,56.1572,10.2034
165,55.9707,9.8483
167,55.4971,8.4419
EOF'
echo -e "${GREEN}✓ Created municipality test data${NC}"
echo ""

echo "Step 4: Checking dependencies..."
kubectl exec ${POD_NAME} -n ${NAMESPACE} -- bash -c '
if python3 -c "import pyspark" 2>/dev/null; then
    echo "PySpark already installed"
    python3 -c "import pyspark; print(\"  Version: \" + pyspark.__version__)"
else
    echo "Installing PySpark and dependencies..."
    pip install -q pyspark==3.4.1 pandas numpy pyarrow 2>&1 | grep -v "WARNING"
    echo "✓ Dependencies installed"
fi
'
echo ""

echo "Step 5: Running test (without HDFS)..."
echo "------------------------------------------------------------"
kubectl exec ${POD_NAME} -n ${NAMESPACE} -- bash -c 'cd /root/code && python3 test_enrichment.py'
TEST_EXIT_CODE=$?
echo "------------------------------------------------------------"
echo ""

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}✓ TEST PASSED - Enrichment logic working correctly!${NC}"
    echo -e "${GREEN}============================================================${NC}"
    echo ""
    echo "What was tested:"
    echo "  ✓ dkArea calculation (lon < 11 → 1, else → 2)"
    echo "  ✓ Municipality code lookup (nearest neighbor)"
    echo "  ✓ Pandas UDF functions"
    echo "  ✓ Spark DataFrame operations"
    echo ""
    echo "Next steps:"
    echo "  1. Review the test output above"
    echo "  2. Test with HDFS: ./test_interactive.sh --hdfs"
    echo "  3. Deploy full batch job: ./build_and_deploy.sh"
else
    echo -e "${RED}============================================================${NC}"
    echo -e "${RED}❌ TEST FAILED${NC}"
    echo -e "${RED}============================================================${NC}"
    echo ""
    echo "Check the error messages above."
    echo "Common issues:"
    echo "  - Missing dependencies"
    echo "  - Java not installed (use Docker image instead)"
    echo "  - Python version incompatibility"
    exit 1
fi

# Optional HDFS test
if [ "$1" == "--hdfs" ]; then
    echo ""
    echo -e "${YELLOW}============================================================${NC}"
    echo -e "${YELLOW}Running HDFS test...${NC}"
    echo -e "${YELLOW}============================================================${NC}"
    echo ""

    kubectl exec ${POD_NAME} -n ${NAMESPACE} -- bash -c '
    cd /root/code
    export TEST_HDFS=true
    export HDFS_NAMENODE=hdfs://namenode-g5:9000
    python3 test_enrichment.py
    '
    HDFS_EXIT_CODE=$?

    if [ $HDFS_EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✓ HDFS test passed${NC}"
    else
        echo -e "${YELLOW}⚠ HDFS test failed (this is normal if HDFS not accessible)${NC}"
    fi
fi

echo ""
echo "============================================================"
echo "Test files remain in pod at: /root/code/"
echo "To run again: kubectl exec -it ${POD_NAME} -n ${NAMESPACE} -- python3 /root/code/test_enrichment.py"
echo "To connect: kubectl exec -it ${POD_NAME} -n ${NAMESPACE} -- bash"
echo "============================================================"