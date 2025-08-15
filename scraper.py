# scraper.py
import os
import requests
import json
import random
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# --- 設定區 ---
DATABASE_URL = os.environ.get('DATABASE_URL')
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
]

def create_session():
    """建立一個帶有標頭的 requests Session"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.6',
    })
    return session

# --- 資料庫函式 ---
def setup_database(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                url VARCHAR(255) UNIQUE,
                start_time VARCHAR(100),
                platform VARCHAR(50),
                image VARCHAR(255)
            );
        """))
        conn.commit()
    print("資料庫資料表 'events' 確認完畢。")

def save_data_to_db(engine, events):
    if not events:
        print("沒有活動資料可以儲存。")
        return
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE events RESTART IDENTITY;"))
        print(f"清空舊資料完成。準備寫入 {len(events)} 筆新資料...")
        for event in events:
            stmt = text("""
                INSERT INTO events (title, url, start_time, platform, image)
                VALUES (:title, :url, :start_time, :platform, :image)
                ON CONFLICT (url) DO NOTHING;
            """)
            conn.execute(stmt, event)
        conn.commit()
    print("資料成功寫入資料庫。")

# --- 各平台爬蟲函式 ( robust version ) ---
def fetch_kktix_events(session):
    print("--- 開始從 KKTIX 抓取活動 ---")
    try:
        response = session.get("https://kktix.com/g/events.json", params={'page': 1, 'order': 'updated_at_desc'}, timeout=20, verify=False)
        response.raise_for_status()
        data = response.json()
        raw_events = data.get('entry', [])
        events = []
        for item in raw_events:
            events.append({
                'title': item.get('title', '標題未知'),
                'url': item.get('url', '#'),
                'start_time': item.get('start', '時間未定').split('T')[0],
                'platform': 'KKTIX',
                'image': item.get('img', '')
            })
        print(f"成功抓取到 {len(events)} 筆 KKTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 KKTIX 時發生嚴重錯誤: {e}")
        traceback.print_exc()
        return []

def fetch_tixcraft_events(session):
    print("--- 開始從 拓元 抓取活動 ---")
    try:
        response = session.get("https://tixcraft.com/activity", timeout=20, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_items = soup.select('.event-list-item a')
        events, seen_urls = [], set()
        for item in event_items:
            href = item.get('href')
            if not href or 'detail' not in href: continue
            full_url = urljoin("https://tixcraft.com/", href)
            if full_url in seen_urls: continue
            title = item.find('h4', class_='event-name')
            date = item.find('p', class_='event-date')
            img_tag = item.find('img')
            if title:
                events.append({
                    'title': title.text.strip(), 'url': full_url,
                    'start_time': date.text.strip() if date else '詳見內文', 'platform': '拓元',
                    'image': img_tag['src'] if img_tag and img_tag.get('src') else ''
                })
                seen_urls.add(full_url)
        print(f"成功解析出 {len(events)} 筆 拓元 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 拓元 時發生嚴重錯誤: {e}")
        traceback.print_exc()
        return []
        
def generic_category_fetcher(session, platform_name, category_map, base_url, selector):
    """通用分類頁面爬蟲"""
    all_events = []
    seen_urls = set()
    for category_name, category_url in category_map.items():
        print(f"正在抓取 {platform_name} 的分類：{category_name}")
        try:
            response = session.get(category_url, timeout=20, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if not href: continue
                full_url = urljoin(base_url, href)
                if full_url in seen_urls: continue
                
                title = (link.find('h5') or link.find('h4') or getattr(link, 'text', '')).strip()
                img_tag = link.find('img')

                if title:
                    all_events.append({
                        'title': title, 'url': full_url, 'start_time': '詳見內文',
                        'platform': platform_name, 'image': urljoin(base_url, img_tag['src']) if img_tag and img_tag.get('src') else ''
                    })
                    seen_urls.add(full_url)
        except Exception as e:
            print(f"警告：抓取 {platform_name} 分類 {category_name} 失敗: {e}")
    return all_events

def fetch_kham_events(session):
    print("--- 開始從 寬宏 抓取活動 ---")
    base_url = "https://kham.com.tw/"
    category_map = {
        "音樂會": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=205",
        "音樂劇": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=80",
        "展覽": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=231",
        "戲劇": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=116",
        "親子": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=129",
    }
    events = generic_category_fetcher(session, "寬宏", category_map, base_url, '.portfolio-item a[href*="PRODUCT_ID"]')
    print(f"成功解析出 {len(events)} 筆 寬宏 活動。")
    return events

def fetch_udn_events(session):
    print("--- 開始從 UDN 抓取活動 ---")
    base_url = "https://tickets.udnfunlife.com/"
    category_map = {
        "展覽": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=231",
        "演唱會": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=77",
        "戲劇": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=116",
        "親子": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=129",
    }
    events = generic_category_fetcher(session, "UDN", category_map, base_url, '.thumbnail a[href*="PRODUCT_ID"]')
    print(f"成功解析出 {len(events)} 筆 UDN 活動。")
    return events
    
def fetch_ibon_events(session):
    print("--- 開始從 iBon 抓取活動 ---")
    try:
        url = "https://ticket.ibon.com.tw/Index/entertainment"
        response = session.get(url, timeout=20, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('.ticket-list-area a')
        events = []
        for link in event_links:
            title = link.find(class_='ticket-title-s')
            img_tag = link.find('img')
            if title and link.get('href'):
                events.append({
                    'title': title.text.strip(),
                    'url': urljoin(url, link['href']),
                    'start_time': '詳見內文', 'platform': 'iBon',
                    'image': img_tag['src'] if img_tag and img_tag.get('src') else ''
                })
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 iBon 時發生嚴重錯誤: {e}")
        traceback.print_exc()
        return []

def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    try:
        url = "https://www.opentix.life/discover"
        response = session.get(url, timeout=20, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('a[href*="/event/"]')
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls: continue
            title_element = link.find('h6') or link.find('h5')
            if title_element and len(title_element.text.strip()) > 2:
                full_url = urljoin(url, href)
                img_tag = link.find('img')
                events.append({
                    'title': title_element.text.strip(),
                    'url': full_url, 'start_time': '詳見內文', 'platform': 'OPENTIX',
                    'image': img_tag['src'] if img_tag and img_tag.get('src') else ''
                })
                seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 OPENTIX 時發生嚴重錯誤: {e}")
        traceback.print_exc()
        return []

# --- 主程式 ---
if __name__ == "__main__":
    if not DATABASE_URL:
        raise Exception("環境變數 DATABASE_URL 未設定")

    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    setup_database(engine)
    session = create_session()
    all_events = []
    
    scraper_tasks = [
        fetch_kktix_events, fetch_tixcraft_events, fetch_kham_events,
        fetch_ibon_events, fetch_udn_events, fetch_opentix_events
    ]
    
    for task in scraper_tasks:
        # 每個任務獨立執行，即使失敗也不會中斷整個流程
        all_events.extend(task(session))

    final_events, processed_urls = [], set()
    for event in all_events:
        if event['url'] not in processed_urls:
            final_events.append(event)
            processed_urls.add(event['url'])
            
    print(f"\n總計抓取到 {len(final_events)} 筆不重複的活動。")
    if final_events:
        save_data_to_db(engine, final_events)
    else:
        print("所有平台都沒有抓取到任何活動，不更新資料庫。")
