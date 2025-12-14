#!/bin/bash

# ================= 配置与变量定义 (Configuration) =================
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ================= 1. 激活 Conda 环境 (Activate Conda) =================
echo -e "${BLUE}[Step 1/3] Activating Conda Environment...${NC}"

# 注意：在 Shell 脚本中激活 Conda 需要先初始化 shell hook
# 尝试初始化 conda
eval "$(conda shell.bash hook)" 2> /dev/null

# 检查是否成功初始化，如果失败通常是因为 conda不在 PATH 中
if [ $? -eq 0 ]; then
    # 激活指定环境
    conda activate hadoop
    if [ $? -eq 0 ]; then
        echo -e "  [OK] Conda environment '${GREEN}hadoop${NC}' activated."
        # 打印一下当前的 python 版本以确认
        echo -e "  [INFO] Current Python: $(which python)"
    else
        echo -e "  ${RED}[ERROR] Failed to activate conda environment 'hadoop'. Please check if it exists.${NC}"
        exit 1
    fi
else
    echo -e "  ${YELLOW}[WARN] Conda command not found or not initialized. Continuing with system Python...${NC}"
fi

echo "------------------------------------------------"

# ================= 2. 环境检查与服务启动 (Service Check) =================
echo -e "${BLUE}[Step 2/3] Checking and Starting Big Data Services...${NC}"

# 定义服务检查函数
# 参数1: JPS显示名称
# 参数2: 启动脚本命令
# 参数3: (可选) 启动后等待秒数
check_and_start() {
    local PROCESS_NAME=$1
    local START_CMD=$2
    local WAIT_TIME=${3:-2} # 默认等待2秒

    # 使用 jps 检查进程
    if jps | grep -q "$PROCESS_NAME"; then
        echo -e "  [OK] ${GREEN}$PROCESS_NAME${NC} is already running."
    else
        echo -e "  ${YELLOW}[WARN] $PROCESS_NAME not found. Starting...${NC}"
        # 执行启动命令
        $START_CMD
        
        # 等待服务初始化
        if [ $WAIT_TIME -gt 0 ]; then
            echo -e "  ... Waiting ${WAIT_TIME}s for initialization ..."
            sleep $WAIT_TIME
        fi
    fi
}

# 1. 检查 Hadoop HDFS
check_and_start "NameNode" "start-dfs.sh"

# 2. 检查 Hadoop YARN
check_and_start "ResourceManager" "start-yarn.sh"

# 3. 检查 HBase Master
check_and_start "HMaster" "start-hbase.sh"

# 4. 检查 HBase Thrift Server
# ThriftServer 启动较慢，单独处理逻辑
if jps | grep -q "ThriftServer"; then
    echo -e "  [OK] ${GREEN}ThriftServer${NC} is already running."
else
    echo -e "  ${YELLOW}[WARN] ThriftServer not found. Starting...${NC}"
    hbase-daemon.sh start thrift
    
    echo -e "  ... Waiting 10s for HBase Thrift to accept connections ..."
    sleep 10 
fi

echo -e "${GREEN}All Services are ready.${NC}"
echo "------------------------------------------------"

# ================= 3. 启动应用 (Start Demo) =================
echo -e "${BLUE}[Step 3/3] Starting Web Application...${NC}"

# 定义 Web 目录
WEB_DIR="src/web"

# 检查目录是否存在
if [ ! -d "$WEB_DIR" ]; then
    echo -e "${RED}[ERROR] Directory '$WEB_DIR' not found! Please check your path.${NC}"
    exit 1
fi

# 使用 pushd 进入目录 (比 cd 更方便管理目录栈)
pushd "$WEB_DIR" > /dev/null

echo -e "  Starting Python App at: ${GREEN}$(pwd)/app.py${NC}"
echo "------------------------------------------------"

# 启动 Python 应用
python app.py

# 应用退出后恢复目录
popd > /dev/null

echo -e "${GREEN}Demo stopped. Good bye!${NC}"