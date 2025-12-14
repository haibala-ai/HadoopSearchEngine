import json
import hashlib
import happybase
import os
import logging
from datetime import datetime
from collections import Counter
from tqdm import tqdm
from src.settings import PROCESSED_DATA_PATH, FAIL_DATA_PATH, LOG_DIR
JSON_FILE = PROCESSED_DATA_PATH / 'extract_data.json'
FAIL_FILE = FAIL_DATA_PATH / 'fail.json'
# =========================================================================
# 组件 0: 日志系统配置 (保持一致)
# =========================================================================

def setup_logger(log_file_path):
    """配置双向日志：文件记录 + 终端显示"""
    logger = logging.getLogger("hbase_importer")
    logger.setLevel(logging.INFO)
    logger.handlers = []  # 清除已有 handler

    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # 文件处理器
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

logger = None

def log_msg(level, tag, msg):
    """统一日志打印函数"""
    full_msg = f"{tag} {msg}"
    
    # 写入日志文件
    if logger:
        if level == 'error':
            logger.error(full_msg)
        else:
            logger.info(full_msg)
            
    # 终端显示 (使用 tqdm.write 防止打断进度条)
    tqdm.write(full_msg)

# =========================================================================
# 组件 1: HBase 导入器
# =========================================================================

class HBaseFileImporter:
    """
    负责将文件信息导入HBase的工具类
    """
    def __init__(self, host='localhost', port=9090, table_name='files'):
        self.host = host
        self.port = port
        self.table_name = table_name
        self.connection = None
        self.table = None

    def connect(self):
        """建立HBase连接"""
        try:
            # log_msg('info', '[INFO]', f"Connecting to HBase at {self.host}:{self.port}...")
            self.connection = happybase.Connection(self.host, self.port)
            self.connection.open()
            log_msg('info', '[Success]', f"Connected to HBase at {self.host}:{self.port}")
        except Exception as e:
            log_msg('error', '[FAIL]', f"Failed to connect to HBase: {e}")
            raise

    def close(self):
        """关闭连接"""
        if self.connection:
            self.connection.close()
            log_msg('info', '[INFO]', "Connection closed.")

    def create_table_if_not_exists(self):
        """检查并创建表"""
        try:
            tables = self.connection.tables()
            if 'index'.encode('utf-8') not in tables:
                log_msg('info', '[INFO]', f"Table 'index' does not exist. Creating...")
                self.connection.create_table(
                    'index',
                    {'p': dict()}  # 定义列族 info
                )
                log_msg('info', '[Success]', f"Table 'index' created.")
            else:
                log_msg('info', '[INFO]', f"Table 'index' already exists.")

            if self.table_name.encode('utf-8') not in tables:
                log_msg('info', '[INFO]', f"Table '{self.table_name}' does not exist. Creating...")
                self.connection.create_table(
                    self.table_name,
                    {'info': dict()}  # 定义列族 info
                )
                log_msg('info', '[Success]', f"Table '{self.table_name}' created.")
            else:
                log_msg('info', '[INFO]', f"Table '{self.table_name}' already exists.")
            
            self.table = self.connection.table(self.table_name)
        except Exception as e:
            log_msg('error', '[FAIL]', f"Error creating/accessing table: {e}")
            raise

    @staticmethod
    def generate_rowkey(url):
        """生成RowKey: 使用URL的MD5值"""
        if not url:
            return None
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def import_data_from_json(self, json_filepath, fail_filepath):
        """读取JSON文件并批量写入HBase"""
        if not os.path.exists(json_filepath):
            log_msg('error', '[FAIL]', f"File not found: {json_filepath}")
            return

        log_msg('info', '[INFO]', f"Loading data from {json_filepath}...")
        
        try:
            with open(json_filepath, 'r', encoding='utf-8') as f:
                data_list = json.load(f)

            if not data_list:
                log_msg('warn', '[WARN]', "JSON file is empty.")
                return

            total_count = len(data_list)
            success_count = 0
            failed_records = []
            
            # 使用 batch 批量插入
            # transaction=True 意味着如果在 send 前发生异常，更改会被丢弃（但这里我们手动 send）
            batch = self.table.batch(batch_size=1000)
            
            log_msg('info', '[INFO]', "Starting import process...")

            for item in tqdm(data_list, desc="Importing to HBase", unit="row"):
                try:
                    url = item.get('url', '')
                    
                    # 1. 基础校验
                    if not url:
                        item['failure_type'] = 'MissingURL'
                        failed_records.append(item)
                        continue

                    row_key = self.generate_rowkey(url)
                    
                    # 2. 数据处理 (List -> String)
                    # 处理 seg_title
                    seg_title_val = item.get('seg_title', [])
                    if isinstance(seg_title_val, list):
                        seg_title_str = ' '.join(seg_title_val)
                    else:
                        seg_title_str = str(seg_title_val) if seg_title_val else ""

                    # 处理 seg_content
                    seg_content_val = item.get('seg_content', [])
                    if isinstance(seg_content_val, list):
                        seg_content_str = ' '.join(seg_content_val)
                    else:
                        seg_content_str = str(seg_content_val) if seg_content_val else ""

                    # 3. 构造数据映射
                    data_map = {
                        b'info:url': item.get('url', '').encode('utf-8'),
                        b'info:title': item.get('title', '').encode('utf-8'),
                        b'info:content': item.get('content', '').encode('utf-8'),
                        b'info:seg_title': seg_title_str.encode('utf-8'),
                        b'info:seg_content': seg_content_str.encode('utf-8')
                    }

                    batch.put(row_key, data_map)
                    success_count += 1

                except Exception as row_e:
                    # 捕获单行处理错误
                    item['failure_type'] = 'ProcessingError'
                    item['error_msg'] = str(row_e)
                    failed_records.append(item)

            # 4. 提交批量操作
            try:
                batch.send()
            except Exception as batch_e:
                log_msg('error', '[FATAL]', f"Batch send failed: {batch_e}")
                # 这里比较严重，如果批量发送失败，可能部分数据已丢失，需要检查 HBase 日志
                raise

            # 5. 结果总结
            log_msg('info', '[INFO]', "--------------------------------------------------")
            log_msg('info', '[Success]', "Import complete.")
            log_msg('info', '[INFO]', f"Total Processed: {total_count}")
            log_msg('info', '[INFO]', f"Successfully Imported: {success_count}")
            
            # 6. 处理失败记录
            if failed_records:
                try:
                    with open(fail_filepath, 'w', encoding='utf-8') as f:
                        json.dump(failed_records, f, ensure_ascii=False, indent=4)
                    
                    # 统计失败原因
                    fail_types = [r.get('failure_type', 'Unknown') for r in failed_records]
                    type_counts = Counter(fail_types)
                    
                    log_msg('info', '[FAIL]', f"Failed Records: {len(failed_records)} (Saved to: {os.path.abspath(fail_filepath)})")
                    log_msg('info', '[INFO]', "Failure Breakdown:")
                    for f_type, count in type_counts.items():
                        log_msg('info', '[INFO]', f"   - {f_type}: {count}")
                except Exception as e:
                    log_msg('error', '[FAIL]', f"Failed to save import_fail.json: {e}")
            else:
                log_msg('info', '[INFO]', "Failed Records: 0")
                
            log_msg('info', '[INFO]', "--------------------------------------------------")

        except json.JSONDecodeError:
            log_msg('error', '[FAIL]', f"Failed to decode JSON from {json_filepath}")
        except Exception as e:
            log_msg('error', '[FAIL]', f"An unexpected error occurred: {e}")

# =========================================================================
# 主程序
# =========================================================================



if __name__ == "__main__":
    # 生成带时间戳的 Log 文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_FILE = os.path.join(LOG_DIR, f'hbase_import_{timestamp}.log')
    
    # 初始化日志
    logger = setup_logger(LOG_FILE)
    log_msg('info', '[INFO]', f"HBase Import Pipeline initialized. Logs saving to: {os.path.abspath(LOG_FILE)}")
    
    # 实例化导入器
    importer = HBaseFileImporter(host='localhost', port=9090, table_name='files')
    
    try:
        # 1. 连接HBase
        importer.connect()
        
        # 2. 确保表存在
        importer.create_table_if_not_exists()
        
        # 3. 导入数据
        importer.import_data_from_json(JSON_FILE, FAIL_FILE)
        
    except Exception as e:
        log_msg('error', '[FATAL]', f"Main process halted: {e}")
    finally:
        # 4. 清理资源
        importer.close()