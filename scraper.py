# scraper.py
import os
import requests
import random
import time
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# 引入 Selenium 相關函式庫
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- 設定區 ---
SCRAPER_VERSION = "v7.0" # <<<<<<<<<<<<< 版本號更新
DATABASE_URL = os.environ.get('DATABASE_URL')
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
]
BASE_HEADERS = {'User-Agent': random.choice(USER_AGENTS),'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.6'}

# --- 函式定義區 (與上一版相同) ---
def create_session():
    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    return session

def setup_database(engine):
    with engine.connect() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, title VARCHAR(255), url VARCHAR(255) UNIQUE, start_time VARCHAR(100), platform VARCHAR(50), image VARCHAR(255));"""))
        conn.commit()
    print("資料庫資料表 'events' 確認完畢。")

def save_data_to_db(engine, events):
    if not events:
        print("沒有新的活動資料可以儲存。")
        return
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE events RESTART IDENTITY;"))
        print(f"清空舊資料完成。準備寫入 {len(events)} 筆新資料...")
        for event in events:
            try:
                stmt = text("""INSERT INTO events (title, url, start_time, platform, image) VALUES (:title, :url, :start_time, :platform, :image) ON CONFLICT (url) DO NOTHING;""")
                conn.execute(stmt, {'title': event.get('title'), 'url': event.get('url'), 'start_time': event.get('start_time'), 'platform': event.get('platform'), 'image': event.get('image')})
            except Exception as e:
                print(f"  警告: 插入活動 '{event.get('title')}' 時失敗: {e}")
        conn.commit()
    print("資料成功寫入資料庫。")

def get_dynamic_page_source(url):
    print(f"  使用 Selenium (無頭瀏覽器) 模式抓取: {url}")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5) # 強制等待渲染
        page_source = driver.page_source
        driver.quit()
        return page_source
    except Exception as e:
        print(f"  Selenium 抓取失敗: {e}");
        if 'driver' in locals(): driver.quit()
        return None

# --- 各平台爬蟲函式 (V7 - 精準解析版) ---
def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    try:
        url = "https://www.opentix.life"
        response = session.get(url, timeout=20, verify=False)
        response.raise_for_status()
        html_content = response.text
        print(f"  成功下載 OPENTIX 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        # 全新、基於真實HTML的選擇器
        event_links = soup.select('a.WebShare_Pcard') 
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls: continue
            title_element = link.find('h5')
            if title_element and len(title_element.text.strip()) > 2:
                full_url = urljoin(url, href)
                img_tag = link.find('img')
                events.append({'title': title_element.text.strip(), 'url': full_url, 'start_time': '詳見內文', 'platform': 'OPENTIX', 'image': img_tag['src'] if img_tag and img_tag.get('src') else ''})
                seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 OPENTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

def fetch_udn_events(session):
    print("--- 開始從 UDN 抓取活動 ---")
    try:
        url = "https://tickets.udnfunlife.com/application/UTK01/UTK0101_01.aspx"
        response = session.get(url, timeout=20, verify=False)
        response.raise_for_status()
        html_content = response.text
        print(f"  成功下載 UDN 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        # 全新、基於真實HTML的選擇器
        event_items = soup.select('ul.product-list > li')
        events = []
        for item in event_items:
            link = item.find('a')
            if not link: continue
            title_element = link.find('h3')
            img_tag = link.find('img')
            if title_element and link.get('href'):
                events.append({'title': title_element.text.strip(),'url': urljoin(url, link['href']),'start_time': '詳見內文','platform': 'UDN','image': urljoin(url, img_tag['src']) if img_tag and img_tag.get('src') else ''})
        print(f"成功解析出 {len(events)} 筆 UDN 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 UDN 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []
    
def fetch_ibon_events(session):
    print("--- 開始從 iBon 抓取活動 ---")
    try:
        url = "https://ticket.ibon.com.tw/Index/entertainment"
        html_content = get_dynamic_page_source(url)
        if not html_content: return []
        print(f"  成功下載 iBon 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        # 全新、基於真實HTML的選擇器
        event_links = soup.select('.item_info > a')
        events = []
        for link in event_links:
            title = link.find(class_='ticket-name')
            img_tag = link.find('img')
            if title and link.get('href'):
                events.append({'title': title.text.strip(),'url': urljoin("https://ticket.ibon.com.tw/", link['href']),'start_time': '詳見內文','platform': 'iBon','image': img_tag['data-src'] if img_tag and img_tag.get('data-src') else ''})
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 iBon 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []
        
# --- 主程式 ---
if __name__ == "__main__":
    print(f"===== 開始執行票券爬蟲 {SCRAPER_VERSION} =====")
    if not DATABASE_URL:
        raise Exception("環境變數 DATABASE_URL 未設定")

    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    
    setup_database(engine)
    session = create_session()
    all_events = []
    
    scraper_tasks = [
        fetch_opentix_events, 
        fetch_udn_events,
        fetch_ibon_events, 
        # 暫時跳過已知會被封鎖或需要更複雜處理的平台
        # fetch_kham_events, 
        # fetch_tixcraft_events, 
        # fetch_kktix_events 
    ]
    
    for task in scraper_tasks:
        all_events.extend(task(session))

    final_events, processed_urls = [], set()
    for event in all_events:
        if event.get('url') and event['url'] not in processed_urls:
            final_events.append(event)
            processed_urls.add(event['url'])
            
    print(f"\n總計抓取到 {len(final_events)} 筆不重複的活動。")
    if final_events:
        save_data_to_db(engine, final_events)
    else:
        print("所有平台都沒有抓取到任何活動，不更新資料庫。")
    print(f"===== 票券爬蟲 {SCRAPER_VERSION} 執行完畢 =====")
