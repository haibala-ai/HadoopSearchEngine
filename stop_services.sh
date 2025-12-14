#!/bin/bash

# ================= 配置 =================
# 设置颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== 开始停止 Hadoop/HBase 服务栈 ===${NC}"

# 定义一个通用函数：检查进程是否存在，如果存在则执行关闭命令
# 参数 $1: jps 中的进程关键词 (用于检测)
# 参数 $2: 关闭命令
check_and_stop() {
    PROCESS_KEY=$1
    STOP_CMD=$2

    # 使用 jps 检查
    if jps | grep -q "$PROCESS_KEY"; then
        echo -e "正在关闭 ${GREEN}$PROCESS_KEY${NC} ..."
        eval "$STOP_CMD"
        
        # 稍微等待一下，让进程有时间清理资源
        echo "等待进程退出..."
        sleep 3 
    else
        echo -e "${GREEN}$PROCESS_KEY${NC} 未运行 (已跳过)。"
    fi
}

# ================= 执行关闭流程 =================

# 1. 关闭 HBase Thrift Server
# 必须最先关闭，因为它依赖 HBase
check_and_stop "ThriftServer" "hbase-daemon.sh stop thrift"

# 2. 关闭 HBase (HMaster)
# HBase 依赖 HDFS 和 ZooKeeper (通常由 HBase 自带或独立管理)
check_and_stop "HMaster" "stop-hbase.sh"

# 3. 关闭 YARN (ResourceManager)
# 计算框架，依赖 HDFS
check_and_stop "NodeManager" "stop-yarn.sh"

# 4. 关闭 HDFS (NameNode)
# 最底层的存储，必须最后关闭
check_and_stop "NameNode" "stop-dfs.sh"

echo -e "${YELLOW}=== 所有服务关闭流程结束 ===${NC}"

# 最后展示一下当前的进程，确认是否干净
echo "当前剩余 Java 进程 (jps):"
jps