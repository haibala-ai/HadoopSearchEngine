import time
import logging
import re  # [新增] 正则表达式
from flask import Flask, render_template, request
from markupsafe import Markup  # [修改] 从 markupsafe 导入
from search_engine import HBaseConnector, SearchEngine
import math

# ================= 配置日志 =================
# 配置日志格式，让终端输出看起来更专业
class ColoredFormatter(logging.Formatter):
    """让终端日志带有颜色，看起来更高级"""
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "[%(asctime)s] [%(levelname)s] %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: green + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

logger = logging.getLogger("WebSearch")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(ColoredFormatter())
logger.addHandler(ch)

# ================= Flask 应用初始化 =================
app = Flask(__name__)

# 全局变量保持连接
connector = None
engine = None

def init_engine():
    """初始化 HBase 连接"""
    global connector, engine
    if not engine:
        try:
            logger.info("正在连接 HBase Thrift Server...")
            connector = HBaseConnector(host='localhost', port=9090)
            connector.connect()
            engine = SearchEngine(connector)
            logger.info("搜索引擎核心模块加载完毕！")
        except Exception as e:
            logger.error(f"HBase 连接失败: {str(e)}")

@app.template_filter('highlight')
def highlight_filter(text, keyword):
    """
    使用正则忽略大小写替换，给关键词加上高亮标签
    注意：为了防止 XSS，生产环境应先转义 text，再替换，这里 Demo 直接处理
    """
    if not keyword or not text:
        return text
    
    # 忽略大小写，保留原文的大小写，包裹在 <mark> 中
    # pattern: (keyword) -> <span class="highlight">\1</span>
    pattern = re.compile(f'({re.escape(keyword)})', re.IGNORECASE)
    
    # 使用 Markup 标记为安全 HTML，否则会被 Flask 转义显示为 &lt;span...
    return Markup(pattern.sub(r'<span class="highlight">\1</span>', text))

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/search')
def search():
    keyword = request.args.get('q', '').strip()
    
    # [新增] 获取当前页码，默认为 1
    try:
        page = int(request.args.get('page', 1))
        if page < 1: page = 1
    except ValueError:
        page = 1

    page_size = 9  # 每页显示 10 条

    if not keyword:
        return render_template('index.html')

    start_time = time.time()
    
    try:
        logger.info(f"搜索请求: '{keyword}' | Page={page}")
        
        # [修改] 调用 search 接收两个返回值
        results, total_count = engine.search(keyword, page=page, page_size=page_size)
        
        # [新增] 计算总页数
        total_pages = math.ceil(total_count / page_size)

    except Exception as e:
        logger.error(f"搜索出错: {str(e)}")
        results = []
        total_count = 0
        total_pages = 0

    elapsed_time = time.time() - start_time
    logger.info(f"耗时: {elapsed_time:.4f}s | 总数: {total_count} | 当前页: {len(results)}")

    return render_template(
        'index.html', 
        results=results, 
        keyword=keyword, 
        count=total_count,  # 这里的 count 是总条数
        time=f"{elapsed_time:.4f}",
        current_page=page,       # [新增] 传给模板
        total_pages=total_pages  # [新增] 传给模板
    )

if __name__ == '__main__':
    init_engine()
    # 启动 Web 服务器，host='0.0.0.0' 允许局域网/WSL宿主机访问
    logger.info("Web 服务器启动在 http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)