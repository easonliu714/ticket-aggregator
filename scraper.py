# scraper.py
import os
import requests
import json
import random
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
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    return session

# --- 資料庫函式 (不變) ---
def setup_database(engine):
    """建立資料表 (如果不存在的話)"""
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
    """將資料儲存到資料庫"""
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
            conn.execute(stmt, {'title': event['title'], 'url': event['url'], 'start_time': event['start_time'], 'platform': event['platform'], 'image': event['image']})
        conn.commit()
        print(f"成功將資料寫入資料庫。")

# --- 各平台爬蟲函式 (修正版) ---

def fetch_kktix_events(session):
    print("開始從 KKTIX 抓取活動...")
    try:
        response = session.get("https://kktix.com/g/events.json?order=updated_at_desc&page=1", timeout=15, verify=False)
        response.raise_for_status()
        data = response.json()
        raw_events = data.get('entry', [])
        events = []
        for event in raw_events:
            start_dt = datetime.fromisoformat(event.get('start', '').replace('Z', '+00:00'))
            formatted_time = start_dt.strftime('%Y-%m-%d %H:%M')
            events.append({
                'title': event.get('title', '標題未知'),
                'url': event.get('url', '#'),
                'start_time': formatted_time,
                'platform': 'KKTIX',
                'image': event.get('img') or ''
            })
        print(f"成功抓取到 {len(events)} 筆 KKTIX 活動。")
        return events
    except requests.RequestException as e:
        print(f"錯誤：請求 KKTIX API 時發生錯誤: {e}")
        return []

def fetch_tixcraft_events(session):
    print("開始從 拓元 tixcraft 抓取活動...")
    try:
        response = session.get("https://tixcraft.com/activity", timeout=15, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_items = soup.select('.event-list-item a')
        print(f"拓元：找到 {len(event_items)} 個可能的活動連結。")
        events = []
        seen_urls = set()
        for item in event_items:
            href = item.get('href')
            if not href or 'detail' not in href:
                continue
            full_url = urljoin("https://tixcraft.com/", href)
            if full_url in seen_urls:
                continue
            title = item.find('h4', class_='event-name')
            date = item.find('p', class_='event-date')
            img_tag = item.find('img')
            if title:
                events.append({
                    'title': title.text.strip(),
                    'url': full_url,
                    'start_time': date.text.strip() if date else '詳見內文',
                    'platform': '拓元',
                    'image': img_tag['src'] if img_tag else ''
                })
                seen_urls.add(full_url)
        print(f"成功解析出 {len(events)} 筆 拓元 活動。")
        return events
    except requests.RequestException as e:
        print(f"錯誤：請求 拓元 網站時發生錯誤: {e}")
        return []

def fetch_kham_events(session):
    print("開始從 寬宏 抓取活動...")
    try:
        url = "https://kham.com.tw/application/UTK01/UTK0101_01.aspx"
        response = session.get(url, timeout=15, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('.portfolio-item a[href*="PRODUCT_ID"]')
        print(f"寬宏：找到 {len(event_links)} 個可能的活動連結。")
        events = []
        for link in event_links:
            title = link.find('h5')
            img_tag = link.find('img')
            if title and link.get('href'):
                events.append({
                    'title': title.text.strip(),
                    'url': urljoin(url, link['href']),
                    'start_time': '詳見內文',
                    'platform': '寬宏',
                    'image': urljoin(url, img_tag['src']) if img_tag else ''
                })
        print(f"成功解析出 {len(events)} 筆 寬宏 活動。")
        return events
    except requests.RequestException as e:
        print(f"錯誤：請求 寬宏 網站時發生錯誤: {e}")
        return []

def fetch_ibon_events(session):
    print("開始從 iBon 抓取活動...")
    try:
        url = "https://ticket.ibon.com.tw/Index/entertainment"
        response = session.get(url, timeout=15, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('.ticket-list-area a')
        print(f"iBon：找到 {len(event_links)} 個可能的活動連結。")
        events = []
        for link in event_links:
            title = link.find(class_='ticket-title-s')
            img_tag = link.find('img')
            if title and link.get('href'):
                events.append({
                    'title': title.text.strip(),
                    'url': urljoin(url, link['href']),
                    'start_time': '詳見內文',
                    'platform': 'iBon',
                    'image': img_tag['src'] if img_tag and img_tag.get('src') else ''
                })
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except requests.RequestException as e:
        print(f"錯誤：請求 iBon 網站時發生錯誤: {e}")
        return []

def fetch_udn_events(session):
    print("開始從 UDN 抓取活動...")
    try:
        url = "https://tickets.udnfunlife.com/application/UTK01/UTK0101_01.aspx"
        response = session.get(url, timeout=15, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('.thumbnail a[href*="PRODUCT_ID"]')
        print(f"UDN：找到 {len(event_links)} 個可能的活動連結。")
        events = []
        for link in event_links:
            title = link.find('h4')
            img_tag = link.find('img')
            if title and link.get('href'):
                events.append({
                    'title': title.text.strip(),
                    'url': urljoin(url, link['href']),
                    'start_time': '詳見內文',
                    'platform': 'UDN',
                    'image': urljoin(url, img_tag['src']) if img_tag else ''
                })
        print(f"成功解析出 {len(events)} 筆 UDN 活動。")
        return events
    except requests.RequestException as e:
        print(f"錯誤：請求 UDN 網站時發生錯誤: {e}")
        return []

def fetch_opentix_events(session):
    print("開始從 OPENTIX 抓取活動...")
    try:
        url = "https://www.opentix.life/discover"
        response = session.get(url, timeout=15, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        event_links = soup.select('a[href*="/event/"]')
        print(f"OPENTIX：找到 {len(event_links)} 個可能的活動連結。")
        events = []
        seen_urls = set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls: continue
            
            title_element = link.find('h6') or link.find('h5') or link.find('h4')
            if title_element and len(title_element.text.strip()) > 2:
                full_url = urljoin(url, href)
                img_tag = link.find('img')
                events.append({
                    'title': title_element.text.strip(),
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'OPENTIX',
                    'image': img_tag['src'] if img_tag and img_tag.get('src') else ''
                })
                seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except requests.RequestException as e:
        print(f"錯誤：請求 OPENTIX 網站時發生錯誤: {e}")
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

    all_events.extend(fetch_kktix_events(session))
    all_events.extend(fetch_tixcraft_events(session))
    all_events.extend(fetch_kham_events(session))
    all_events.extend(fetch_ibon_events(session))
    all_events.extend(fetch_udn_events(session))
    all_events.extend(fetch_opentix_events(session))

    final_events = []
    processed_urls = set()
    for event in all_events:
        if event['url'] not in processed_urls:
            final_events.append(event)
            processed_urls.add(event['url'])
            
    if final_events:
        save_data_to_db(engine, final_events)
    else:
        print("所有平台都沒有抓取到任何活動，不更新資料庫。")
