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

# --- 設定區 ---
SCRAPER_VERSION = "v3.1" # 加入版本號
DATABASE_URL = os.environ.get('DATABASE_URL')
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
]
BASE_HEADERS = {
    'User-Agent': random.choice(USER_AGENTS),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://www.google.com/'
}

# --- 函式定義區 ---
def create_session():
    return requests.Session()

def setup_database(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, title VARCHAR(255), url VARCHAR(255) UNIQUE, start_time VARCHAR(100), platform VARCHAR(50), image VARCHAR(255));
        """))
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
                conn.execute(stmt, event)
            except Exception as e:
                print(f"  警告: 插入活動 '{event.get('title')}' 時失敗: {e}")
        conn.commit()
    print("資料成功寫入資料庫。")

def fetch_with_retry(session, url, headers, retries=2, delay=5):
    """帶有重試機制的請求函式"""
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, timeout=20, verify=False)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP 錯誤 (嘗試 {attempt + 1}/{retries}): {e.response.status_code} for {url}")
            if e.response.status_code in [403, 404] and attempt == retries - 1:
                raise # 最後一次嘗試仍然失敗，則拋出異常
            time.sleep(delay * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f"  網路請求錯誤 (嘗試 {attempt + 1}/{retries}): {e}")
            time.sleep(delay * (attempt + 1))
    return None # 所有重試均失敗

# --- 各平台爬蟲函式 (V3) ---
def fetch_kktix_events(session):
    print("--- 開始從 KKTIX 抓取活動 ---")
    try:
        kktix_headers = BASE_HEADERS.copy()
        kktix_headers['Accept'] = 'application/json' # API 請求標頭
        response = fetch_with_retry(session, "https://kktix.com/g/events.json", headers=kktix_headers)
        if not response: return []
        data = response.json()
        raw_events = data.get('entry', [])
        events = [{'title': item.get('title', '標題未知'),'url': item.get('url', '#'),'start_time': item.get('start', '時間未定').split('T')[0],'platform': 'KKTIX','image': item.get('img', '')} for item in raw_events]
        print(f"成功抓取到 {len(events)} 筆 KKTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 KKTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

def fetch_tixcraft_events(session):
    print("--- 開始從 拓元 抓取活動 ---")
    try:
        response = fetch_with_retry(session, "https://tixcraft.com/activity", headers=BASE_HEADERS)
        if not response: return []
        soup = BeautifulSoup(response.text, 'html.parser')
        event_items = soup.select('.ticket-list-item > a, .event-list-item > a') # 兩種可能的 class name
        events, seen_urls = [], set()
        for item in event_items:
            href = item.get('href')
            if not href or 'detail' not in href: continue
            full_url = urljoin("https://tixcraft.com/", href)
            if full_url in seen_urls: continue
            title = item.find('h4', class_='event-name') or item.find(class_='ticket-name')
            date = item.find('p', class_='event-date') or item.find(class_='ticket-time')
            img_tag = item.find('img')
            if title:
                events.append({'title': title.text.strip(),'url': full_url,'start_time': date.text.strip() if date else '詳見內文','platform': '拓元','image': img_tag['src'] if img_tag and img_tag.get('src') else ''})
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
            response = session.get(category_url, headers=BASE_HEADERS, timeout=20, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if not href: continue
                full_url = urljoin(base_url, href)
                if full_url in seen_urls: continue
                title_element = link.find('h5') or link.find('h4')
                title = title_element.text.strip() if title_element else ''
                img_tag = link.find('img')
                if title:
                    all_events.append({'title': title,'url': full_url,'start_time': '詳見內文','platform': platform_name,'image': urljoin(base_url, img_tag['src']) if img_tag and img_tag.get('src') else ''})
                    seen_urls.add(full_url)
        except Exception as e:
            print(f"  警告：抓取 {platform_name} 分類 {category_name} 失敗: {e}")
        time.sleep(random.uniform(1, 2))
    return all_events

def fetch_kham_events(session):
    print("--- 開始從 寬宏 抓取活動 ---")
    events = generic_category_fetcher(session, "寬宏", {"所有活動": "https://kham.com.tw/application/UTK01/UTK0101_01.aspx"}, "https://kham.com.tw/", '.portfolio-item a[href*="PRODUCT_ID"]')
    print(f"成功解析出 {len(events)} 筆 寬宏 活動。")
    return events

def fetch_udn_events(session):
    print("--- 開始從 UDN 抓取活動 ---")
    events = generic_category_fetcher(session, "UDN", {"所有活動": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_01.aspx"}, "https://tickets.udnfunlife.com/", '.thumbnail a[href*="PRODUCT_ID"]')
    print(f"成功解析出 {len(events)} 筆 UDN 活動。")
    return events
    
def fetch_ibon_events(session):
    print("--- 開始從 iBon 抓取活動 ---")
    try:
        url = "https://ticket.ibon.com.tw/Index/entertainment"
        response = session.get(url, headers=BASE_HEADERS, timeout=20, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('.ticket-card > a')
        events = []
        for link in event_links:
            title = link.find(class_='ticket-title-s')
            img_tag = link.find('img')
            if title and link.get('href'):
                events.append({'title': title.text.strip(),'url': urljoin(url, link['href']),'start_time': '詳見內文','platform': 'iBon','image': img_tag['src'] if img_tag and img_tag.get('src') else ''})
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 iBon 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    try:
        url = "https://www.opentix.life/discover/popular" # 經過測試，這個網址更穩定
        response = session.get(url, headers=BASE_HEADERS, timeout=20, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('a[data-testid="event-card-link"]')
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls: continue
            title_element = link.find('h6')
            if title_element and len(title_element.text.strip()) > 2:
                full_url = urljoin("https://www.opentix.life", href)
                img_tag = link.find('img')
                events.append({'title': title_element.text.strip(),'url': full_url,'start_time': '詳見內文','platform': 'OPENTIX','image': img_tag['src'] if img_tag and img_tag.get('src') else ''})
                seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 OPENTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

# --- 主程式 ---
if __name__ == "__main__":
    print(f"===== 開始執行票券爬蟲 v{SCRAPER_VERSION} =====")
    if not DATABASE_URL:
        raise Exception("環境變數 DATABASE_URL 未設定")

    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    setup_database(engine)
    session = create_session()
    all_events = []
    
    scraper_tasks = [
        fetch_opentix_events, fetch_tixcraft_events, fetch_kham_events,
        fetch_udn_events, fetch_ibon_events, fetch_kktix_events 
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
    print(f"===== 票券爬蟲 v{SCRAPER_VERSION} 執行完畢 =====")
