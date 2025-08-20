# scraper.py

import os
import random
import time
import traceback
import re
from datetime import datetime
import json

from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import requests
import cloudscraper
from urllib.parse import urljoin

# --- 設定區 ---
SCRAPER_VERSION = "v9.6"
DATABASE_URL = os.environ.get('DATABASE_URL')

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
]

def extract_info_from_title(title):
    info = {'date': '詳內文', 'location': '詳內文'}
    date_patterns = [
        r'(\d{4}[./]\d{1,2}[./]\d{1,2})',
        r'(\d{1,2}[./]\d{1,2}[./]\d{4})',
        r'(\d{1,2}月\d{1,2}日)',
    ]
    for pattern in date_patterns:
        match = re.search(pattern, title)
        if match:
            info['date'] = match.group(1).strip()
            break
    location_patterns = [
        r'[@＠]([^\s@#]+)',
        r'在([^，,。\s]{2,8})',
        r'於([^，,。\s]{2,8})',
    ]
    for pattern in location_patterns:
        match = re.search(pattern, title)
        if match:
            location = match.group(1).strip()
            if len(location) > 1 and location not in ['台灣', '台北', '高雄']:
                info['location'] = location
                break
    return info

def debug_html_content(platform_name, url, html_content):
    if html_content:
        content_after_head = re.split(r'</head>', html_content, flags=re.IGNORECASE)[-1]
        preview = content_after_head[:500].strip()
        print(f"[DEBUG] {platform_name} HTML預覽 ({url}):")
        print(f"前500字內容: {preview}")
        print("=" * 50)

def create_session(use_cloudscraper=False):
    if use_cloudscraper:
        session = cloudscraper.create_scraper()
    else:
        session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.6',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
    })
    return session

def setup_database(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                url VARCHAR(255) UNIQUE,
                start_time VARCHAR(100),
                platform VARCHAR(50),
                image VARCHAR(255),
                event_type VARCHAR(100),
                location VARCHAR(200),
                event_date VARCHAR(200)
            );
        """))
        try:
            conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS event_type VARCHAR(100);"))
            conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS location VARCHAR(200);"))
            conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS event_date VARCHAR(200);"))
        except:
            pass
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
                # 修正：標題、location欄位過長剪裁避免 SQL 錯誤
                title = event.get('title') or '詳見內文'
                if len(title) > 255:
                    title = title[:252] + '...'
                location = event.get('location') or '詳內文'
                if len(location) > 200:
                    location = location[:197] + '...'

                stmt = text("""
                    INSERT INTO events (title, url, start_time, platform, image, event_type, location, event_date)
                    VALUES (:title, :url, :start_time, :platform, :image, :event_type, :location, :event_date)
                    ON CONFLICT (url) DO NOTHING;
                """)
                conn.execute(stmt, {
                    'title': title,
                    'url': event.get('url'),
                    'start_time': event.get('start_time') or '詳內文',
                    'platform': event.get('platform'),
                    'image': event.get('image') or '',
                    'event_type': event.get('event_type') or '其他',
                    'location': location,
                    'event_date': event.get('event_date') or '詳內文'
                })
            except Exception as e:
                print(f" 警告: 插入活動 '{title}' 時失敗: {e}")
                # 每次插入錯誤，需回滾才能繼續
                conn.rollback()
        conn.commit()
        print("資料成功寫入資料庫。")

def get_all_event_types_from_db(engine):
    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT DISTINCT event_type FROM events WHERE event_type IS NOT NULL"))
            types = sorted([row[0] for row in result])
            return ['所有類型'] + types
        except Exception as e:
            print(f"取得活動類型清單失敗: {e}")
            return ['所有類型']

# --- OPENTIX 強化爬取含補足詳情頁方案 ---
def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    base_urls = ["https://www.opentix.life", "https://www.opentix.life/event"]
    events, seen = [], set()

    def fetch_event_details(event_url):
        """訪問詳情頁補全資料，若可取標題等更新event"""
        try:
            resp = session.get(event_url, timeout=25, verify=False)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            # 範例：嘗試抓標題（依實際頁面調整）
            title_elem = soup.select_one('h1') or soup.select_one('.event-title')
            title = title_elem.get_text(strip=True) if title_elem else None
            # 可擴展抓類型、圖片等欄位
            # 這裡示意只抓標題與可能的圖片
            img_elem = soup.select_one('.event-header img') or soup.select_one('img')
            img_url = img_elem.get('src') if img_elem else ''
            tag_elem = soup.select_one('.event-category')  # 假設類型可能在此
            event_type = tag_elem.get_text(strip=True) if tag_elem else None
            return {
                'title': title,
                'image': img_url,
                'event_type': event_type,
            }
        except Exception as e:
            print(f"  詳情頁抓取失敗: {event_url}，原因: {e}")
            return {}

    for url in base_urls:
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            }
            resp = session.get(url, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            debug_html_content("OPENTIX", url, resp.text)

            soup = BeautifulSoup(resp.text, 'html.parser')
            card_links = soup.find_all('a', class_='oa-card-img-title')
            print(f"發現活動卡片(oa-card-img-title)：{len(card_links)}")
            for card in card_links:
                href = card.get('href', '')
                if not href or href in seen:
                    continue
                full_url = urljoin(url, href)
                title_div = card.find('div', class_='text')
                title = title_div.get_text(strip=True) if title_div else None
                if not title or len(title) <= 3:
                    continue
                img = card.find('img')
                img_url = img.get('src') if img else ''
                tag_elem = card.find('div', class_='oa-cardTypeTag eventTag')
                event_type = tag_elem.get_text(strip=True) if tag_elem else '其他'
                info = extract_info_from_title(title)

                # 詳情頁補充，避免資料缺漏
                extra = fetch_event_details(full_url)
                if extra.get('title'):
                    title = extra['title']
                if extra.get('image'):
                    img_url = extra['image']
                if extra.get('event_type'):
                    event_type = extra['event_type']

                events.append({
                    'title': title or '詳見內文',
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'OPENTIX',
                    'image': img_url or '',
                    'event_type': event_type or '其他',
                    'location': info['location'] or '詳內文',
                    'event_date': info['date'] or '詳內文'
                })
                seen.add(href)

            # 輔助擴增a[href*="/event/"]連結
            extra_links = soup.select('a[href*="/event/"]')
            for link_a in extra_links:
                href = link_a.get('href', '')
                if not href or href in seen:
                    continue
                full_url = urljoin(url, href)
                aria_label = link_a.get('aria-label', '').strip()
                title_text = aria_label if aria_label and len(aria_label) > 3 else link_a.text.strip()
                if not title_text or len(title_text) <= 3:
                    continue
                info = extract_info_from_title(title_text)
                # 嘗試詳情頁補充，但嚴格在此不重複添加圖片、類型以免過度
                extra = fetch_event_details(full_url)
                if extra.get('title'):
                    title_text = extra['title']

                events.append({
                    'title': title_text or '詳見內文',
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'OPENTIX',
                    'image': extra.get('image') or '',
                    'event_type': extra.get('event_type') or '其他',
                    'location': info['location'] or '詳內文',
                    'event_date': info['date'] or '詳內文'
                })
                seen.add(href)

            break
        except Exception as e:
            print(f"OPENTIX URL {url} 爬取失敗: {e}")
            continue
    print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
    return events

# --- 其他平台函式保持不變，這裡省略示意 --- 
# (請使用前文已提供的相同fetch_kham_events, fetch_kktix_events, fetch_tixcraft_events, fetch_ibon_events, fetch_udn_events, fetch_ticket_events, fetch_eventgo_events)

def fetch_kham_events(session):
    # 同前文實作，這裡省略
    return []

def fetch_kktix_events(session):
    # 同前文實作，這裡省略
    return []

def fetch_tixcraft_events(session):
    # 同前文實作，這裡省略
    return []

def fetch_ibon_events(session):
    # 同前文實作，這裡省略
    return []

def fetch_udn_events(session):
    # 同前文實作，這裡省略
    return []

def fetch_ticket_events(session):
    # 同前文實作，這裡省略
    return []

def fetch_eventgo_events(session):
    # 同前文實作，這裡省略
    return []


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
    fetch_funcs = [
        fetch_opentix_events,
        fetch_kham_events,
        fetch_kktix_events,
        fetch_tixcraft_events,
        fetch_ibon_events,
        fetch_udn_events,
        fetch_ticket_events,
        fetch_eventgo_events,
    ]
    for func in fetch_funcs:
        try:
            all_events.extend(func(session))
        except Exception as e:
            print(f"執行 {func.__name__} 發生錯誤: {e}")
        time.sleep(random.uniform(1, 2.2))

    # 過濾去重
    final_events, processed_urls = [], set()
    for event in all_events:
        url = event.get('url')
        if url and url not in processed_urls:
            final_events.append(event)
            processed_urls.add(url)

    print(f"\n總計抓取到 {len(final_events)} 筆不重複的活動。")

    # 寫入資料庫
    if final_events:
        save_data_to_db(engine, final_events)
    else:
        print("所有平台都沒有抓取到任何活動，不更新資料庫。")

    # 類別清單由資料庫動態取得，給前端用API
    all_types = get_all_event_types_from_db(engine)
    print("目前活動類型總覽:", all_types)

    print(f"===== 票券爬蟲 {SCRAPER_VERSION} 執行完畢 =====")
