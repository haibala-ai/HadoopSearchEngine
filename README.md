# HadoopSearchEngine

HadoopSearchEngine 是一个基于伪分布式 HBase 和 Hadoop 构建的文件搜索系统。该项目演示了从非结构化数据（Word, PDF, Excel）提取信息、存储到 HBase、利用 MapReduce 构建倒排索引，并最终通过 Web 界面提供搜索服务的完整流程。

## 项目文件结构

```
HadoopSearchEngine/
├── run_server.sh               # [脚本] 一键启动 Web 搜索服务器（会自动检查并启动 Hadoop/HBase 环境）
├── run_workflow.sh             # [脚本] 一键运行完整数据处理工作流（ETL -> HBase导入 -> MapReduce索引构建）
├── stop_services.sh            # [脚本] 一键停止所有 Hadoop/HBase 相关服务
├── bin/                        # [目录] 存放 ETL 过程的日志文件
├── logs/                       # [目录] 存放编译后的 Java MapReduce 类文件或 JAR 包
├── config/                     # [目录] 配置文件
│   └── stopwords_full.txt      #        中英文停用词表，用于分词时过滤无意义词汇
├── data/                       # [目录] 数据存储
│   ├── failures/               #        ETL 过程中处理失败的文件记录
│   ├── processed/              #        ETL 处理后的中间结果 (JSON 格式)
│   └── raw/                    #        原始数据源
│       ├── data.json           #        元数据文件
│       └── files/              #        待处理的实际文件 (PDF, Word, Excel 等)
└── src/                        # [源码] 核心源代码
    ├── settings.py             #        全局配置文件（路径、HBase 表名等）
    ├── crawler/                # [模块] 数据爬虫
    │   ├── spider.py           #        爬虫：爬取 PDF/Word/Excel
    ├── etl/                    #        [模块] Extract-Transform-Load 数据清洗与加载
    │   ├── data_extractor.py   #        文档解析器：读取 PDF/Word/Excel，进行分词和清洗
    │   └── hbase_import.py     #        HBase 导入器：将清洗后的数据写入 HBase 原数据表
    ├── mapreduce/              #        [模块] 离线计算
    │   └── HBaseInvertedIndex.java #    MapReduce 程序：读取 HBase 原数据，构建倒排索引表
    └── web/                    #        [模块] Web 搜索前端
        ├── app.py              #        Flask 应用入口，处理 HTTP 请求
        ├── search_engine.py    #        搜索核心逻辑：连接 HBase，执行查询和相关性排序
        ├── static/             #        静态资源 (CSS, JS)
        └── templates/          #        HTML 模板
```

## 脚本功能与运行指南

本项目提供了三个核心 Shell 脚本，用于简化开发和部署流程。在运行之前，请确保已配置好 Hadoop 和 HBase 的伪分布式环境，并安装了必要的 Python 依赖（推荐使用 Conda 环境 `hadoop`）。

### `run_server.sh`
**功能**：
该脚本用于启动搜索系统的 Web 界面。
1.  **环境激活**：自动激活名为 `hadoop` 的 Conda 环境。
2.  **服务检查**：检查 Hadoop (HDFS, YARN) 和 HBase (HMaster, ThriftServer) 是否正在运行。如果未运行，脚本会尝试自动启动它们。
3.  **启动 Web**：启动 Flask 应用 (`src/web/app.py`)，默认在 `5000` 端口提供服务。

**运行**：
```bash
./run_server.sh
```

### `run_workflow.sh`
**功能**：
该脚本串联了整个后端数据处理流程，适合在数据更新时运行。
1.  **环境准备**：激活 Conda 环境，检查并启动大数据基础设施。
2.  **数据提取 (ETL)**：运行 `src/etl/data_extractor.py`，从 `data/raw/files` 中解析文档，分词并生成中间 JSON。
3.  **数据导入**：运行 `src/etl/hbase_import.py`，将清洗后的数据存入 HBase 的文档表。
4.  **索引构建**：提交 MapReduce 任务 (`src/mapreduce/HBaseInvertedIndex.java`)，计算倒排索引并写入 HBase 索引表。

**运行**：
```bash
./run_workflow.sh
```

### `stop_services.sh`
**功能**：
该脚本用于优雅地关闭所有相关的大数据服务，防止数据损坏。
它会按照依赖顺序的反序关闭服务：
1.  **ThriftServer** (HBase 接口)
2.  **HMaster** (HBase 主节点)
3.  **NodeManager / ResourceManager** (YARN)
4.  **NameNode / DataNode** (HDFS)

**运行**：
```bash
./stop_services.sh
```

## 项目优点

*   **完整的 Demo 演示**：
    项目不仅仅是后端逻辑，还包含了一个基于 Flask 的 Web 前端。用户可以直接通过浏览器输入关键词，实时体验基于 HBase 的搜索效果，直观展示了大数据技术在搜索场景下的应用。

*   **详细的提取设计与相关性计算**：
    *   **多格式支持**：`data_extractor.py` 集成了 `fitz` (PyMuPDF), `python-docx`, `openpyxl` 等库，能够处理 PDF, Word, Excel 等多种真实业务中常见的非结构化文件。
    *   **中文分词**：引入 `jieba` 分词库，并配合停用词表 (`stopwords_full.txt`) 进行精细的文本清洗。
    *   **倒排索引**：利用 MapReduce 并行计算能力构建倒排索引，这是搜索引擎核心技术之一，保证了检索的高效性。

*   **全栈自动化脚本**：
    项目提供了 `run_server.sh`, `run_workflow.sh`, `stop_services.sh` 等全套 Shell 脚本。这些脚本封装了繁琐的环境变量设置、服务状态检测（使用 `jps`）、依赖启动顺序管理等细节。开发者无需手动逐个启动 HDFS、YARN、HBase 和 Python 脚本，极大地降低了操作门槛和出错概率。
