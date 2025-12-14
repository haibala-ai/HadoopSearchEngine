#!/bin/bash

# ================= Configuration Section =================
# Get project root directory
PROJECT_ROOT=$(pwd)

# [IMPORTANT] Add current directory to PYTHONPATH for importing src.settings
export PYTHONPATH=$PROJECT_ROOT

# Color codes for terminal output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "Project Root Directory: ${BLUE}$PROJECT_ROOT${NC}"
echo "------------------------------------------------"

# ================= 1. Activate Conda Environment =================
echo -e "${BLUE}[Step 1/6] Activating Conda Environment...${NC}"

eval "$(conda shell.bash hook)" 2> /dev/null
if [ $? -eq 0 ]; then
    # Activate specified environment
    CONDA_ENV_NAME="hadoop"
    conda activate "$CONDA_ENV_NAME"
    
    if [ $? -eq 0 ]; then
        echo -e "  [OK] Conda environment '${GREEN}$CONDA_ENV_NAME${NC}' activated."
        # Print Python version for verification
        echo -e "  [INFO] Current Python: $(which python)"
    else
        echo -e "  ${RED}[ERROR] Failed to activate conda environment '$CONDA_ENV_NAME'. Please check if it exists.${NC}"
        exit 1
    fi
else
    echo -e "  ${YELLOW}[WARN] Conda command not found or not initialized. Continuing with system Python...${NC}"
fi

echo "------------------------------------------------"

# ================= 2. Environment Check & Service Startup =================
echo -e "${BLUE}[Step 2/6] Checking and Starting Services...${NC}"

check_and_start() {
    PROCESS_NAME=$1
    START_CMD=$2
    
    if jps | grep -q "$PROCESS_NAME"; then
        echo -e "  ${GREEN}[OK] $PROCESS_NAME is running.${NC}"
    else
        echo -e "  ${YELLOW}[WARN] $PROCESS_NAME not found. Starting...${NC}"
        eval "$START_CMD"
        sleep 2
    fi
}

check_and_start "NameNode" "start-dfs.sh"
check_and_start "ResourceManager" "start-yarn.sh"
check_and_start "HMaster" "start-hbase.sh"

if ! jps | grep -q "ThriftServer"; then
    echo -e "  ${YELLOW}[WARN] ThriftServer not found. Starting...${NC}"
    hbase-daemon.sh start thrift
    echo "  Waiting for HBase & Thrift to initialize (10s)..."
    sleep 10  # Allow sufficient time for HBase startup to prevent connection refused
else
    echo -e "  ${GREEN}[OK] ThriftServer is running.${NC}"
fi

echo -e "${GREEN}Services are ready.${NC}"
echo "------------------------------------------------"

# ================= 3. Data Extraction =================
echo -e "${BLUE}[Step 3/6] Running Data Extractor...${NC}"

# Use Python from Conda environment
python src/etl/data_extractor.py --mode 0

if [ $? -ne 0 ]; then
    echo -e "${RED}Data extraction failed, workflow terminated.${NC}"
    exit 1
fi

echo "------------------------------------------------"

# ================= 4. Import Data to HBase =================
echo -e "${BLUE}[Step 4/6] Importing Data to HBase...${NC}"

python src/etl/hbase_import.py

if [ $? -ne 0 ]; then
    echo -e "${RED}HBase import failed, workflow terminated.${NC}"
    exit 1
fi

echo "------------------------------------------------"

# ================= 5. Compile Java MapReduce Job =================
echo -e "${BLUE}[Step 5/6] Compiling MapReduce Job...${NC}"

pushd src/mapreduce > /dev/null
if [ $? -ne 0 ]; then
    echo -e "${RED}[ERROR] Failed to change directory to src/mapreduce.${NC}"
    exit 1
fi

rm -f *.class
javac HBaseInvertedIndex.java

if [ $? -ne 0 ]; then
    echo -e "${RED}Java compilation failed! Please check the code.${NC}"
    popd > /dev/null
    exit 1
fi

# Package JAR file
jar cf Indexer.jar HBaseInvertedIndex*.class

# Move to bin directory
mkdir -p $PROJECT_ROOT/bin
mv Indexer.jar $PROJECT_ROOT/bin/

# Clean up
rm *.class

# Return to previous directory
popd > /dev/null

echo "  ${GREEN}[OK] MapReduce Job compiled and moved to bin/Indexer.jar${NC}"
echo "------------------------------------------------"

# ================= 6. Submit Hadoop Job =================
echo -e "${BLUE}[Step 6/6] Submitting Hadoop Job...${NC}"

# Submit job to Hadoop
hadoop jar bin/Indexer.jar HBaseInvertedIndex

if [ $? -eq 0 ]; then
    echo -e "${GREEN}==============================================${NC}"
    echo -e "${GREEN}   Workflow Completed Successfully! 🚀   ${NC}"
    echo -e "${GREEN}==============================================${NC}"
else
    echo -e "${RED}MapReduce job execution failed.${NC}"
    exit 1
fi