from pathlib import Path

# 1. 动态获取当前文件 (src/settings.py) 的绝对路径
# .parent 得到 src/
# .parent.parent 得到 项目根目录/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 2. 定义各子目录常量
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"
BIN_DIR = PROJECT_ROOT / "bin"

# 3. 具体文件路径 (可选)
STOPWORDS_PATH = CONFIG_DIR / "stopwords_full.txt"
RAW_DATA_PATH = DATA_DIR / "raw"
PROCESSED_DATA_PATH = DATA_DIR / "processed"
FAIL_DATA_PATH = DATA_DIR / "failures"
