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
SCRAPER_VERSION = "v9.3"
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
                stmt = text("""
                    INSERT INTO events (title, url, start_time, platform, image, event_type, location, event_date)
                    VALUES (:title, :url, :start_time, :platform, :image, :event_type, :location, :event_date)
                    ON CONFLICT (url) DO NOTHING;
                """)
                conn.execute(stmt, {
                    'title': event.get('title'),
                    'url': event.get('url'),
                    'start_time': event.get('start_time'),
                    'platform': event.get('platform'),
                    'image': event.get('image'),
                    'event_type': event.get('event_type'),
                    'location': event.get('location'),
                    'event_date': event.get('event_date')
                })
            except Exception as e:
                print(f" 警告: 插入活動 '{event.get('title')}' 時失敗: {e}")
        conn.commit()
        print("資料成功寫入資料庫。")

def safe_get_text(element, default="詳內文"):
    return element.get_text(strip=True) if element and hasattr(element, 'get_text') and element.get_text(strip=True) else default

# --- OPENTIX 最終修正版 ---
def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    urls = [
        "https://www.opentix.life",
        "https://www.opentix.life/events",
        "https://www.opentix.life/event"
    ]
    events, seen = [], set()
    for url in urls:
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
            # 找所有活動卡片
            card_links = soup.find_all('a', class_='oa-card-img-title')
            print(f"發現活動卡片(oa-card-img-title)：{len(card_links)}")
            for card in card_links:
                href = card.get('href', '')
                if not href or href in seen:
                    continue
                full_url = urljoin(url, href)
                img = card.find('img')
                img_url = img.get('src') if img else ''
                tag_elem = card.find('div', class_='oa-cardTypeTag eventTag')
                event_type = tag_elem.get_text(strip=True) if tag_elem else '其他'
                title_div = card.find('div', class_='text')
                title = title_div.get_text(strip=True) if title_div else None
                if not title or len(title) <= 3:
                    continue
                info = extract_info_from_title(title)
                events.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'OPENTIX',
                    'image': img_url,
                    'event_type': event_type,
                    'location': info['location'],
                    'event_date': info['date']
                })
                seen.add(href)
            # 若有活動，直接結束
            if events:
                break
        except Exception as e:
            print(f"OPENTIX URL {url} 爬取失敗: {e}")
            continue
    print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
    return events

# --- 其他平台（保持原有邏輯）---
def fetch_kham_events(session):
    print("--- 開始從 寬宏 抓取活動 ---")
    base_url = "https://kham.com.tw/"
    category_map = {
        "音樂會/演唱會": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=205",
        "展覽/博覽": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=231",
        "戲劇表演": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=116",
        "親子活動": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=129",
    }
    all_events, seen_urls = [], set()
    for category_name, category_url in category_map.items():
        print(f" 抓取分類：{category_name}")
        try:
            resp = session.get(category_url, timeout=20)
            resp.raise_for_status()
            debug_html_content("寬宏", category_url, resp.text)
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = soup.select('ul#product_list li') or soup.select('.product-item') or soup.select('div[class*="product"]')
            for item in items:
                title_elem = item.select_one('div.product_name a') or item.select_one('a[class*="title"]') or item.select_one('a')
                title = title_elem.text.strip() if title_elem else ''
                link = title_elem.get('href') if title_elem else None
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                if full_url in seen_urls:
                    continue
                img_elem = item.select_one('div.product_img img') or item.select_one('img')
                image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
                info = extract_info_from_title(title)
                all_events.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': '寬宏',
                    'image': image,
                    'event_type': category_name,
                    'location': info['location'],
                    'event_date': info['date']
                })
                seen_urls.add(full_url)
            time.sleep(random.uniform(1, 2))
        except Exception as e:
            print(f"寬宏抓取 {category_name} 失敗: {e}")
    print(f"成功解析出 {len(all_events)} 筆 寬宏 活動。")
    return all_events

def fetch_kktix_events(session):
    print("--- 開始從 KKTIX 抓取活動 ---")
    try:
        cs_session = cloudscraper.create_scraper()
        url = "https://kktix.com/events"
        resp = cs_session.get(url, timeout=20)
        resp.raise_for_status()
        debug_html_content("KKTIX", url, resp.text)
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('ul.event-list li') or soup.select('div[class*="event"]') or soup.select('article')
        events, seen = [], set()
        for item in event_items:
            link = item.select_one('a')
            if not link:
                continue
            href = link.get('href')
            full_url = urljoin("https://kktix.com/", href) if href else None
            if not full_url or full_url in seen:
                continue
            title_elem = item.select_one('.event-title') or item.select_one('h3') or item.select_one('h2') or link
            title = title_elem.text.strip() if title_elem else ''
            img_elem = item.select_one('img')
            image = img_elem.get('data-src') or img_elem.get('src', '') if img_elem else ''
            if title and len(title) > 3:
                info = extract_info_from_title(title)
                events.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'KKTIX',
                    'image': image,
                    'event_type': '其他',
                    'location': info['location'],
                    'event_date': info['date']
                })
                seen.add(full_url)
        print(f"成功解析出 {len(events)} 筆 KKTIX 活動。")
        return events
    except Exception as e:
        print(f"KKTIX 爬取失敗: {e}")
        return []

def fetch_tixcraft_events(session):
    print("--- 開始從 拓元 抓取活動 ---")
    try:
        cs_session = cloudscraper.create_scraper()
        cs_session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://tixcraft.com/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        url = "https://tixcraft.com/activity"
        resp = cs_session.get(url, timeout=20)
        resp.raise_for_status()
        debug_html_content("拓元", url, resp.text)
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('div.activity-block') or soup.select('div[class*=\"activity\"]') or soup.select('article')
        events, seen = [], set()
        for item in event_items:
            link = item.select_one('a[href*=\"/activity/detail\"]') or item.select_one('a')
            if not link:
                continue
            title_elem = item.select_one('.activity-name') or item.select_one('h3') or item.select_one('h2') or link
            title = title_elem.text.strip() if title_elem else ''
            img_elem = item.select_one('img')
            image = urljoin(url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            full_url = urljoin("https://tixcraft.com/", link['href'])
            if title and full_url not in seen and len(title) > 3:
                info = extract_info_from_title(title)
                events.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': '拓元',
                    'image': image,
                    'event_type': '其他',
                    'location': info['location'],
                    'event_date': info['date']
                })
                seen.add(full_url)
        print(f"成功解析出 {len(events)} 筆 拓元 活動。")
        return events
    except Exception as e:
        print(f"拓元 爬取失敗: {e}")
        return []

def fetch_ibon_events(session):
    print("--- 開始從 iBon 抓取活動 ---")
    url = "https://ticket.ibon.com.tw/Index/entertainment"
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        debug_html_content("iBon", url, resp.text)
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('div.ticket-item') or soup.select('div[class*=\"item\"]') or soup.select('article')
        events, seen = [], set()
        for item in event_items:
            link = item.select_one('a[href*=\"/activity/detail\"]') or item.select_one('a')
            if not link:
                continue
            title_elem = item.select_one('div.ticket-title') or item.select_one('h3') or item.select_one('.title') or link
            title = title_elem.text.strip() if title_elem else ''
            img_elem = item.select_one('img')
            image = urljoin(url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            if title and len(title) > 3:
                full_url = urljoin("https://ticket.ibon.com.tw/", link['href'])
                if full_url in seen:
                    continue
                info = extract_info_from_title(title)
                events.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'iBon',
                    'image': image,
                    'event_type': '其他',
                    'location': info['location'],
                    'event_date': info['date']
                })
                seen.add(full_url)
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"iBon 爬取失敗: {e}")
        return []

def fetch_udn_events(session):
    return []
def fetch_ticket_events(session):
    return []
def fetch_eventgo_events(session):
    return []

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
        all_events.extend(func(session))
        time.sleep(random.uniform(1, 2.2))
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
