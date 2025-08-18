import os
import time
import random
from datetime import datetime
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SCRAPER_VERSION = "v9.0"
DATABASE_URL = os.environ.get('DATABASE_URL')

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
]

def create_session():
    session = requests.Session()
    retry = Retry(total=3, read=3, connect=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504, 403, 401))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.6',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'no-cache',
        'DNT': '1',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none'
    })
    session.verify = True
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
            image VARCHAR(255)
        );
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
                stmt = text("""INSERT INTO events (title, url, start_time, platform, image)
                               VALUES (:title, :url, :start_time, :platform, :image)
                               ON CONFLICT (url) DO NOTHING;""")
                conn.execute(stmt, {
                    'title': event.get('title'),
                    'url': event.get('url'),
                    'start_time': event.get('start_time'),
                    'platform': event.get('platform'),
                    'image': event.get('image')
                })
            except Exception as e:
                print(f"警告: 插入活動 '{event.get('title')}' 時失敗: {e}")
        conn.commit()
        print("資料成功寫入資料庫。")

def safe_get_text(element, default="詳內文"):
    try:
        return element.get_text(strip=True) if element else default
    except Exception:
        return default

def fetch_opentix_events(sess):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    url = "https://www.opentix.life"
    try:
        resp = sess.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_links = soup.select('a[href*="/event/"]')
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls or not href.startswith('/event/'):
                continue
            title_text = link.text.strip()
            h_title = link.find('h5') or link.find('h6')
            if h_title:
                title_text = h_title.text.strip()
            if len(title_text) <= 3:
                continue
            full_url = urljoin(url, href)
            img_tag = link.find('img')
            image = urljoin(url, img_tag['src']) if img_tag and img_tag.get('src') else ''
            events.append({'title': title_text, 'url': full_url, 'start_time': '詳見內文', 'platform': 'OPENTIX', 'image': image})
            seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"OPENTIX 爬取失敗: {e}")
        return []

def fetch_kham_events(sess):
    print("--- 開始從 寬宏 抓取活動 ---")
    result = []
    category_map = {
        "音樂會/演唱會": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=205",
        "展覽/博覽": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=231",
        "戲劇表演": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=116",
        "親子活動": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=129",
    }
    base_url = "https://kham.com.tw/"
    for category_name, category_url in category_map.items():
        try:
            resp = sess.get(category_url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = soup.select('ul#product_list li')
            for item in items:
                t_link = item.select_one('div.product_name a')
                title = t_link.text.strip() if t_link else ''
                link = t_link.get('href') if t_link else None
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                img_elem = item.select_one('div.product_img img.lazy')
                image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
                result.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': '寬宏',
                    'image': image
                })
            print(f"{category_name} 類別解析出 {len(items)} 活動。")
        except Exception as e:
            print(f"寬宏-{category_name}抓取失敗: {e}")
        time.sleep(random.uniform(0.5, 1.2))
    print(f"成功解析出 {len(result)} 筆 寬宏 活動。")
    return result

def fetch_udn_events(sess):
    print("--- 開始從 UDN 抓取活動 ---")
    result = []
    category_map = {
        "展覽/博覽": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=231",
        "音樂會/演唱會": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=77",
        "戲劇表演": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=116",
    }
    base_url = "https://tickets.udnfunlife.com/"
    for category_name, category_url in category_map.items():
        try:
            resp = sess.get(category_url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = soup.select('ul#product_list li')
            for item in items:
                t_link = item.select_one('div.product_name a')
                title = t_link.text.strip() if t_link else ''
                link = t_link.get('href') if t_link else None
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                img_elem = item.select_one('div.product_img img.lazy')
                image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
                result.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': 'UDN',
                    'image': image
                })
            print(f"{category_name} 類別解析出 {len(items)} 活動。")
        except Exception as e:
            print(f"UDN-{category_name}抓取失敗: {e}")
        time.sleep(random.uniform(0.5, 1.2))
    print(f"成功解析出 {len(result)} 筆 UDN 活動。")
    return result

def fetch_ibon_events(sess):
    print("--- 開始從 iBon 抓取活動 ---")
    url = "https://ticket.ibon.com.tw/Index/entertainment"
    try:
        resp = sess.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('div.ticket-item')
        events = []
        for item in event_items:
            link = item.select_one('a[href*="/activity/detail"]')
            if not link:
                continue
            title_elem = item.select_one('div.ticket-info div.ticket-title')
            title = title_elem.text.strip() if title_elem else ''
            img_elem = item.select_one('img')
            image = urljoin(url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            if not title:
                continue
            full_url = urljoin("https://ticket.ibon.com.tw/", link['href'])
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'iBon',
                'image': image
            })
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"iBon 爬取失敗: {e}")
        return []

def fetch_kktix_events(sess):
    print("--- 開始從 KKTIX 抓取活動 ---")
    url = "https://kktix.com/events"
    try:
        resp = sess.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('ul.event-list li')
        events, seen_urls = [], set()
        for item in event_items:
            link = item.select_one('a.event-link')
            if not link:
                continue
            href = link.get('href')
            full_url = urljoin("https://kktix.com/", href)
            if full_url in seen_urls:
                continue
            title_elem = item.select_one('.event-title')
            title = safe_get_text(title_elem, link.text)
            img_elem = item.select_one('img.event-image')
            image = img_elem.get('data-src') or img_elem.get('src', '') if img_elem else ''
            if not title or len(title) <= 3:
                continue
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'KKTIX',
                'image': image
            })
            seen_urls.add(full_url)
        print(f"成功解析出 {len(events)} 筆 KKTIX 活動。")
        return events
    except Exception as e:
        print(f"KKTIX 爬取失敗: {e}")
        return []

def fetch_tixcraft_events(sess):
    print("--- 開始從 拓元 抓取活動 ---")
    url = "https://tixcraft.com/activity"
    try:
        resp = sess.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('div.activity-block')
        events = []
        for item in event_items:
            link = item.select_one('a[href*="/activity/detail"]')
            if not link:
                continue
            title_elem = item.select_one('.activity-name')
            title = title_elem.text.strip() if title_elem else link.text.strip()
            img_elem = item.select_one('.activity-thumbnail img')
            image = urljoin(url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            full_url = urljoin("https://tixcraft.com/", link['href'])
            if not title or len(title) <= 3:
                continue
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': '拓元',
                'image': image
            })
        print(f"成功解析出 {len(events)} 筆 拓元 活動。")
        return events
    except Exception as e:
        print(f"拓元 爬取失敗: {e}")
        return []

def fetch_ticket_events(sess):
    print("--- 開始從 年代售票 抓取活動 ---")
    result = []
    category_map = {
        "音樂會/演唱會": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=1",
        "展覽/博覽": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=3",
        "戲劇表演": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=2",
        "親子活動": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=4",
    }
    base_url = "https://www.ticket.com.tw/"
    for category_name, category_url in category_map.items():
        try:
            resp = sess.get(category_url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = soup.select('ul#product_list li')
            for item in items:
                t_link = item.select_one('div.product_name a')
                title = t_link.text.strip() if t_link else ''
                link = t_link.get('href') if t_link else None
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                img_elem = item.select_one('div.product_img img.lazy')
                image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
                result.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': '年代',
                    'image': image
                })
            print(f"{category_name} 類別解析出 {len(items)} 活動。")
        except Exception as e:
            print(f"年代-{category_name}抓取失敗: {e}")
        time.sleep(random.uniform(0.5, 1.2))
    print(f"成功解析出 {len(result)} 筆 年代 活動。")
    return result

def fetch_eventgo_events(sess):
    print("--- 開始從 Event GO 抓取活動 ---")
    url = "https://eventgo.bnextmedia.com.tw/event/list"
    base_url = "https://eventgo.bnextmedia.com.tw/"
    try:
        resp = sess.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        event_items = soup.select('div.event-card, li.event-item')
        events = []
        for item in event_items:
            link = item.select_one('a[href*="event/detail"]')
            if not link:
                continue
            href = link.get('href')
            full_url = urljoin(base_url, href)
            title_elem = item.select_one('h3.event-title, .event-title')
            title = title_elem.text.strip() if title_elem else link.text.strip()
            img_elem = item.select_one('img.event-img, img')
            image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            if not title or len(title) <= 3:
                continue
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'Event GO',
                'image': image
            })
        print(f"成功解析出 {len(events)} 筆 Event GO 活動。")
        return events
    except Exception as e:
        print(f"Event GO 爬取失敗: {e}")
        return []

if __name__ == "__main__":
    print(f"===== 開始執行票券爬蟲 {SCRAPER_VERSION} =====")
    if not DATABASE_URL:
        raise Exception("環境變數 DATABASE_URL 未設定")
    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    setup_database(engine)
    sess = create_session()
    all_events = []
    all_events.extend(fetch_opentix_events(sess))
    all_events.extend(fetch_kham_events(sess))
    all_events.extend(fetch_udn_events(sess))
    all_events.extend(fetch_ibon_events(sess))
    all_events.extend(fetch_kktix_events(sess))
    all_events.extend(fetch_tixcraft_events(sess))
    all_events.extend(fetch_ticket_events(sess))
    all_events.extend(fetch_eventgo_events(sess))
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
