import happybase
import struct
import sys

class HBaseConnector:
    def __init__(self, host='localhost', port=9090):
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = happybase.Connection(self.host, port=self.port)
            self.connection.open()
            print(f"[INFO] 成功连接到 HBase Thrift Server ({self.host}:{self.port})")
        except Exception as e:
            print(f"[ERROR] 连接 HBase 失败: {e}")
            print("请确保已运行 'hbase-daemon.sh start thrift'")
            sys.exit(1)

    def get_table(self, table_name):
        if not self.connection:
            self.connect()
        return self.connection.table(table_name)

    def close(self):
        if self.connection:
            self.connection.close()

class SearchEngine:
    def __init__(self, connector):
        self.connector = connector
        self.index_table = self.connector.get_table('index')
        self.files_table = self.connector.get_table('files')

    def search(self, keyword, page=1, page_size=10):
        """
        分页搜索
        返回: (results, total_count)
        """
        print(f"\n[SEARCH]正在检索关键词: '{keyword}' (Page {page}) ...")
        
        # 1. 查 Index 表 (获取所有相关的 URL 和 分数)
        row = self.index_table.row(keyword)
        if not row:
            return [], 0  # 返回空列表和总数0

        # 2. 解析 Index
        hits = []
        for col_key, val_bytes in row.items():
            full_col_name = col_key.decode('utf-8')
            if full_col_name.startswith('p:'):
                url = full_col_name[2:]
                try:
                    # 兼容双精度字节流或字符串
                    if len(val_bytes) == 8:
                        score = struct.unpack('>d', val_bytes)[0]
                    else:
                        score = float(val_bytes.decode('utf-8'))
                except:
                    score = 0.0
                
                hits.append({'url': url, 'score': score})

        # 3. 按分数降序排序
        hits.sort(key=lambda x: x['score'], reverse=True)
        
        # [新增] 计算总条数
        total_count = len(hits)

        # [新增] 执行内存切片 (只获取当前页的 url)
        # 举例: page=1 -> [0:10], page=2 -> [10:20]
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        # 如果超出范围，返回空
        if start_idx >= total_count:
            return [], total_count

        current_page_hits = hits[start_idx:end_idx]
        print(f"[INFO] 命中总数: {total_count}, 当前页获取详情: {len(current_page_hits)} 条")

        # 4. 批量去 Files 表查详情 (只查这10条)
        urls = [h['url'] for h in current_page_hits]
        files_data = self.files_table.rows(urls)
        files_map = {key.decode('utf-8'): data for key, data in files_data}

        # 5. 组装结果
        results = []
        for hit in current_page_hits:
            url = hit['url']
            score = hit['score']
            file_row = files_map.get(url, {})
            
            title_bytes = file_row.get(b'info:title') 
            title = title_bytes.decode('utf-8') if title_bytes else "无标题"

            content_bytes = file_row.get(b'info:content') 
            content = content_bytes.decode('utf-8') if content_bytes else "无内容"

            url_bytes = file_row.get(b'info:url') 
            url = url_bytes.decode('utf-8') if url_bytes else ""

            results.append({
                'score': score,
                'url': url,
                'title': title,
                'content': content
            })

        return results, total_count  # 返回元组

def main():
    connector = HBaseConnector(host='localhost', port=9090)
    connector.connect()

    try:
        engine = SearchEngine(connector)

        while True:
            print("="*60)
            keyword = input("请输入查询关键词 (输入 'q' 退出): ").strip()
            
            if keyword.lower() == 'q':
                break
            if not keyword:
                continue

            results = engine.search(keyword)

            if not results:
                print(f"[RESULT] 未找到关于 '{keyword}' 的结果。")
            else:
                print(f"\n======== 搜索结果 (Top {len(results)}) ========")
                for i, res in enumerate(results):
                    print(f"[{i+1}] Score: {res['score']:.4f}")
                    print(f"    Title: {res['title']}")
                    print(f"    URL:   {res['url']}")
                    snippet = res['content'][:100].replace('\n', ' ') + "..."
                    print(f"    Snippet: {snippet}")
                    print("-" * 40)

    except KeyboardInterrupt:
        print("\n程序已终止")
    finally:
        connector.close()

if __name__ == "__main__":
    main()