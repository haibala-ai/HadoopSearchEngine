import os
import json
import time
import random
import logging
import re
from urllib.parse import urljoin, urlparse
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm

# 尝试导入项目配置
try:
    from src.settings import RAW_DATA_PATH
except ImportError:
    # 如果作为独立脚本运行，回退到默认路径
    RAW_DATA_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "raw"

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class USTCCrawler:
    def __init__(self, base_dir=None):
        self.base_dir = base_dir if base_dir else RAW_DATA_PATH
        self.files_dir = self.base_dir / "files"
        self.json_path = self.base_dir / "data.json"
        
        # 确保目录存在
        self.files_dir.mkdir(parents=True, exist_ok=True)
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
        ]
        
        self.proxies = [
            # 示例代理，实际使用时请确保代理可用
            # "http://58.241.88.18:800",
        ]
        
        self.file_data = self._load_existing_data()
        self.target_extensions = ('.pdf', '.doc', '.docx', '.csv', '.pptx', '.xlsx')

    def _load_existing_data(self):
        if self.json_path.exists():
            try:
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return []
        return []

    def _get_random_headers(self):
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive"
        }

    def _get_proxy(self):
        if not self.proxies:
            return None
        return {"http": random.choice(self.proxies)}

    def sanitize_filename(self, filename):
        return re.sub(r'[<>:"/\\|?*]', '_', filename)

    def download_file(self, file_url, referer_url):
        filename = self.sanitize_filename(os.path.basename(file_url))
        # 如果文件名过长，截断
        if len(filename) > 200:
            filename = filename[-200:]
            
        save_path = self.files_dir / filename
        
        # 检查是否已下载
        if save_path.exists():
            logger.info(f"文件已存在，跳过: {filename}")
            # 确保记录在 metadata 中
            self._add_to_metadata(file_url, filename)
            return

        try:
            logger.info(f"正在下载: {file_url}")
            response = requests.get(
                file_url, 
                timeout=30, 
                headers=self._get_random_headers(), 
                proxies=self._get_proxy(),
                stream=True
            )
            
            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"下载成功: {filename}")
                self._add_to_metadata(file_url, filename)
            else:
                logger.error(f"下载失败 {response.status_code}: {file_url}")
                
        except Exception as e:
            logger.error(f"下载异常: {e}")

    def _add_to_metadata(self, url, filename):
        # 避免重复添加
        relative_path = f"/files/{filename}"
        for item in self.file_data:
            if item.get('path') == relative_path:
                return
                
        self.file_data.append({
            "url": url,
            "path": relative_path,
            "download_time": time.strftime("%Y-%m-%d %H:%M:%S")
        })
        self.save_metadata()

    def save_metadata(self):
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(self.file_data, f, ensure_ascii=False, indent=4)

    def crawl_page_selenium(self, url):
        """使用 Selenium 抓取动态页面"""
        logger.info(f"正在抓取页面 (Selenium): {url}")
        options = Options()
        options.add_argument('--headless') # 无头模式
        options.add_argument('--disable-gpu')
        
        driver = None
        try:
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            # 等待加载
            time.sleep(3) 
            
            html_content = driver.page_source
            self.parse_and_download(html_content, url)
            
        except Exception as e:
            logger.error(f"Selenium 抓取失败: {e}")
        finally:
            if driver:
                driver.quit()

    def crawl_page_requests(self, url):
        """使用 Requests 抓取静态页面"""
        logger.info(f"正在抓取页面 (Requests): {url}")
        try:
            response = requests.get(
                url, 
                timeout=15, 
                headers=self._get_random_headers(),
                proxies=self._get_proxy()
            )
            if response.status_code == 200:
                self.parse_and_download(response.text, url)
            else:
                logger.error(f"页面请求失败 {response.status_code}: {url}")
        except Exception as e:
            logger.error(f"Requests 抓取异常: {e}")

    def parse_and_download(self, html_content, base_url):
        soup = BeautifulSoup(html_content, 'html.parser')
        links = soup.find_all('a', href=True)
        
        found_files = 0
        for link in links:
            href = link['href']
            if href.startswith('javascript') or href.startswith('#'):
                continue
                
            full_url = urljoin(base_url, href)
            
            # 检查是否是目标文件类型
            if full_url.lower().endswith(self.target_extensions):
                self.download_file(full_url, base_url)
                found_files += 1
                time.sleep(random.uniform(0.5, 1.5)) # 礼貌延时
        
        logger.info(f"页面处理完成，找到 {found_files} 个文件")

    def post_process_data(self):
        """数据清洗与校验 (对应原 json processor.py)"""
        logger.info("开始执行数据清洗与校验...")
        
        if not self.json_path.exists():
            logger.warning("数据文件不存在")
            return

        with open(self.json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        unique_paths = set()
        valid_items = []
        
        for item in data:
            path = item.get('path')
            if not path:
                continue
                
            # 1. 去重
            if path in unique_paths:
                continue
            unique_paths.add(path)
            
            # 2. 校验文件是否存在
            # path 格式为 /files/xxx.pdf，需要转为绝对路径
            # 去掉开头的 /files/ 或者 / 
            filename = os.path.basename(path)
            abs_path = self.files_dir / filename
            
            if abs_path.exists():
                valid_items.append(item)
            else:
                logger.warning(f"文件丢失，已从记录中移除: {abs_path}")

        # 保存清洗后的数据
        self.file_data = valid_items
        self.save_metadata()
        logger.info(f"清洗完成。剩余有效记录: {len(valid_items)}")

    def run(self, urls, use_selenium=True):
        for url in urls:
            if use_selenium:
                self.crawl_page_selenium(url)
            else:
                self.crawl_page_requests(url)
            
        # 最后执行数据清洗
        self.post_process_data()

if __name__ == "__main__":
    # 待抓取的 URL 列表
    target_urls = [
        'https://ustcnet.ustc.edu.cn/33489/list.psp',
        'https://ustcnet.ustc.edu.cn/33492/list.htm',
        # 可以添加更多 URL
    ]
    
    crawler = USTCCrawler()
    crawler.run(target_urls, use_selenium=True)
