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
import cloudscraper # <<<<<<<<<<<<< 引入終極武器

# 引入 Selenium 相關函式庫
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- 設定區 ---
SCRAPER_VERSION = "v8.0" # <<<<<<<<<<<<< 版本號更新
DATABASE_URL = os.environ.get('DATABASE_URL')

# --- 函式定義區 ---
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
    print(f"  使用 Selenium 抓取: {url}")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = None
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)
        page_source = driver.page_source
        driver.quit()
        return page_source
    except Exception as e:
        print(f"  Selenium 抓取失敗: {e}");
        if driver: driver.quit()
        return None

# --- 各平台爬蟲函式 (V8) ---
def fetch_kktix_events(scraper):
    print("--- 開始從 KKTIX 抓取活動 ---")
    try:
        response = scraper.get("https://kktix.com/g/events.json")
        response.raise_for_status()
        print(f"  成功下載 KKTIX API，長度: {len(response.text)} bytes")
        data = response.json()
        raw_events = data.get('entry', [])
        events = [{'title': item.get('title', '標題未知'),'url': item.get('url', '#'),'start_time': item.get('start', '時間未定').split('T')[0],'platform': 'KKTIX','image': item.get('img', '')} for item in raw_events]
        print(f"成功解析出 {len(events)} 筆 KKTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 KKTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

def fetch_tixcraft_events(scraper):
    print("--- 開始從 拓元 抓取活動 ---")
    try:
        response = scraper.get("https://tixcraft.com/activity")
        response.raise_for_status()
        html_content = response.text
        print(f"  成功下載 拓元 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = soup.select('a[href*="/activity/detail/"]') # 100% 複製成功經驗
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if full_url := urljoin("https://tixcraft.com/", href) not in seen_urls:
                title = link.find('h4', class_='event-name')
                if title:
                    events.append({'title': title.text.strip(),'url': full_url,'start_time': '詳見內文','platform': '拓元','image': ''})
                    seen_urls.add(full_url)
        print(f"成功解析出 {len(events)} 筆 拓元 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 拓元 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

def generic_category_fetcher(session, platform_name, category_map, base_url, selector):
    all_events, seen_urls = [], set()
    for category_name, category_url in category_map.items():
        print(f"  正在抓取 {platform_name} 的分類：{category_name}")
        try:
            response = session.get(category_url, timeout=20, verify=False)
            response.raise_for_status()
            html_content = response.text
            print(f"    成功下載頁面，長度: {len(html_content)} bytes")
            soup = BeautifulSoup(html_content, 'html.parser')
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if not href: continue
                full_url = urljoin(base_url, href)
                if full_url in seen_urls: continue
                title = link.text.strip()
                if title and len(title) > 3:
                    all_events.append({'title': title,'url': full_url,'start_time': '詳見內文','platform': platform_name,'image': ''})
                    seen_urls.add(full_url)
        except Exception as e:
            print(f"  警告：抓取 {platform_name} 分類 {category_name} 失敗: {e}")
        time.sleep(random.uniform(1.5, 3))
    return all_events

def fetch_kham_events(session):
    print("--- 開始從 寬宏 抓取活動 ---")
    base_url = "https://kham.com.tw/"
    category_map = {"所有活動": "https://kham.com.tw/application/UTK01/UTK0101_01.aspx"}
    events = generic_category_fetcher(session, "寬宏", category_map, base_url, 'a[href*="UTK0201"]')
    print(f"成功解析出 {len(events)} 筆 寬宏 活動。")
    return events

def fetch_udn_events(session):
    print("--- 開始從 UDN 抓取活動 ---")
    base_url = "https://tickets.udnfunlife.com/"
    category_map = {"所有活動": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_01.aspx"}
    events = generic_category_fetcher(session, "UDN", category_map, base_url, 'a[href*="UTK0201"]')
    print(f"成功解析出 {len(events)} 筆 UDN 活動。")
    return events
    
def fetch_ibon_events(session):
    print("--- 開始從 iBon 抓取活動 ---")
    try:
        url = "https://ticket.ibon.com.tw/Index/entertainment"
        html_content = get_dynamic_page_source(url)
        if not html_content: return []
        print(f"  成功下載 iBon 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = soup.select('a[href*="/activity/detail"]')
        events = []
        for link in event_links:
            title = link.find(class_='ticket-title-s') or link.find(class_='ticket-name')
            if title and link.get('href'):
                events.append({'title': title.text.strip(),'url': urljoin("https://ticket.ibon.com.tw/", link['href']),'start_time': '詳見內文','platform': 'iBon','image': ''})
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 iBon 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []
        
def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    try:
        url = "https://www.opentix.life"
        response = session.get(url, timeout=20, verify=False)
        response.raise_for_status()
        html_content = response.text
        print(f"  成功下載 OPENTIX 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = soup.select('a[href*="/event/"]')
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls or not href.startswith('/event/'): continue
            title = link.text.strip()
            if len(title) > 3: # 篩掉一些空標題或無意義的連結
                full_url = urljoin(url, href)
                events.append({'title': title, 'url': full_url, 'start_time': '詳見內文', 'platform': 'OPENTIX', 'image': ''})
                seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 OPENTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

# --- 主程式 ---
if __name__ == "__main__":
    print(f"===== 開始執行票券爬蟲 {SCRAPER_VERSION} =====")
    if not DATABASE_URL: raise Exception("環境變數 DATABASE_URL 未設定")
    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    setup_database(engine)
    
    # 建立兩種 session
    requests_session = create_session()
    cloudscraper_instance = cloudscraper.create_scraper()
    
    all_events = []
    
    # 根據網站的防禦等級，使用不同的武器
    all_events.extend(fetch_opentix_events(requests_session))
    all_events.extend(fetch_kham_events(requests_session))
    all_events.extend(fetch_udn_events(requests_session))
    all_events.extend(fetch_ibon_events(requests_session))
    all_events.extend(fetch_tixcraft_events(cloudscraper_instance))
    all_events.extend(fetch_kktix_events(cloudscraper_instance))
    
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
