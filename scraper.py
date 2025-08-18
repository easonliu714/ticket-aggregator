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
import cloudscraper  # 用於繞過403

# 引入 Selenium 相關函式庫
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- 設定區 ---
SCRAPER_VERSION = "v8.4"  # 更新版本
DATABASE_URL = os.environ.get('DATABASE_URL')
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
]
BASE_HEADERS = {'User-Agent': random.choice(USER_AGENTS),'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.6'}

# --- 函式定義區 ---
def create_session(use_cloudscraper=False):
    if use_cloudscraper:
        return cloudscraper.create_scraper()
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
    
def get_dynamic_page_source(url, wait_time=10):
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
        time.sleep(wait_time)
        page_source = driver.page_source
        driver.quit()
        return page_source
    except Exception as e:
        print(f"  Selenium 抓取失敗: {e}");
        if driver: driver.quit()
        return None

# --- 通用分類抓取函式 ---
def generic_category_fetcher(session, platform_name, category_map, base_url, item_selector, title_selector, image_selector, use_cloudscraper=False):
    all_events, seen_urls = [], set()
    scraper = create_session(use_cloudscraper)
    for category_name, category_url in category_map.items():
        print(f"  正在抓取 {platform_name} 的分類：{category_name}")
        try:
            response = scraper.get(category_url, timeout=20)  # 移除verify=False
            response.raise_for_status()
            html_content = response.text
            print(f"    成功下載頁面，長度: {len(html_content)} bytes")
            soup = BeautifulSoup(html_content, 'html.parser')
            items = soup.select(item_selector)
            for item in items:
                title_elem = item.select_one(title_selector)
                title = title_elem.text.strip() if title_elem else ''
                link = title_elem.get('href') if title_elem else item.select_one('a').get('href') if item.select_one('a') else None
                if not link or not title or len(title) <= 3 or "more" in title.lower(): continue
                full_url = urljoin(base_url, link)
                if full_url in seen_urls: continue
                img_elem = item.select_one(image_selector)
                image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
                all_events.append({'title': title, 'url': full_url, 'start_time': '詳見內文', 'platform': platform_name, 'image': image})
                seen_urls.add(full_url)
        except Exception as e:
            print(f"  警告：抓取 {platform_name} 分類 {category_name} 失敗: {e}")
        time.sleep(random.uniform(1.5, 3))
    return all_events

# --- OPENTIX (保留) ---
def fetch_opentix_events(session):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    try:
        url = "https://www.opentix.life"
        response = session.get(url, timeout=20)  # 移除verify
        response.raise_for_status()
        html_content = response.text
        print(f"  成功下載 OPENTIX 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = soup.select('a[href*="/event/"]')
        events, seen_urls = [], set()
        for link in event_links:
            href = link.get('href')
            if not href or href in seen_urls or not href.startswith('/event/'): continue
            title_text = link.text.strip()
            h_title = link.find('h5') or link.find('h6')
            if h_title:
                title_text = h_title.text.strip()
            if len(title_text) > 3:
                full_url = urljoin(url, href)
                img_tag = link.find('img')
                image = urljoin(url, img_tag['src']) if img_tag and img_tag.get('src') else ''
                events.append({'title': title_text, 'url': full_url, 'start_time': '詳見內文', 'platform': 'OPENTIX', 'image': image})
                seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 OPENTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

# --- 宽宏 (修正image get data-src) ---
def fetch_kham_events(session):
    print("--- 開始從 寬宏 抓取活動 ---")
    base_url = "https://kham.com.tw/"
    category_map = {
        "音樂會/演唱會": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=205",
        "展覽/博覽": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=231",
        "戲劇表演": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=116",
        "親子活動": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=129",
    }
    events = generic_category_fetcher(session, "寬宏", category_map, base_url, 
                                      item_selector='ul#product_list li', 
                                      title_selector='div.product_name a', 
                                      image_selector='div.product_img img.lazy')
    print(f"成功解析出 {len(events)} 筆 寬宏 活動。")
    return events

# --- UDN (類似) ---
def fetch_udn_events(session):
    print("--- 開始從 UDN 抓取活動 ---")
    base_url = "https://tickets.udnfunlife.com/"
    category_map = {
        "展覽/博覽": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=231",
        "音樂會/演唱會": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=77",
        "戲劇表演": "https://tickets.udnfunlife.com/application/UTK01/UTK0101_03.aspx?Category=116",
    }
    events = generic_category_fetcher(session, "UDN", category_map, base_url, 
                                      item_selector='ul#product_list li', 
                                      title_selector='div.product_name a', 
                                      image_selector='div.product_img img.lazy')
    print(f"成功解析出 {len(events)} 筆 UDN 活動。")
    return events

# --- iBon (修正選擇器) ---
def fetch_ibon_events(session):
    print("--- 開始從 iBon 抓取活動 ---")
    try:
        url = "https://ticket.ibon.com.tw/Index/entertainment"
        html_content = get_dynamic_page_source(url, wait_time=15)
        if not html_content: return []
        print(f"  成功下載 iBon 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        event_items = soup.select('div.ticket-item')
        events = []
        for item in event_items:
            link = item.select_one('a[href*="/activity/detail"]')
            if not link: continue
            title_elem = item.select_one('div.ticket-info div.ticket-title')
            title = title_elem.text.strip() if title_elem else ''
            img_elem = item.select_one('img.ticket-img')
            image = urljoin(url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            if title:
                full_url = urljoin("https://ticket.ibon.com.tw/", link['href'])
                events.append({'title': title, 'url': full_url, 'start_time': '詳見內文', 'platform': 'iBon', 'image': image})
        print(f"成功解析出 {len(events)} 筆 iBon 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 iBon 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

# --- KKTIX (移除verify=False) ---
def fetch_kktix_events(session):
    print("--- 開始從 KKTIX 抓取活動 ---")
    base_url = "https://kktix.com/"
    try:
        url = "https://kktix.com/events"
        scraper = create_session(use_cloudscraper=True)
        response = scraper.get(url, timeout=20)  # 移除verify
        response.raise_for_status()
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        event_items = soup.select('ul.event-list li')
        events, seen_urls = [], set()
        for item in event_items:
            link = item.select_one('a.event-link')
            if not link: continue
            href = link.get('href')
            full_url = urljoin(base_url, href)
            if full_url in seen_urls: continue
            title = item.select_one('.event-title').text.strip()
            img_elem = item.select_one('img.event-image')
            image = img_elem['src'] if img_elem else ''
            if title:
                events.append({'title': title, 'url': full_url, 'start_time': '詳見內文', 'platform': 'KKTIX', 'image': image})
                seen_urls.add(full_url)
        print(f"成功解析出 {len(events)} 筆 KKTIX 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 KKTIX 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

# --- 拓元 (修正) ---
def fetch_tixcraft_events(session):
    print("--- 開始從 拓元 抓取活動 ---")
    try:
        url = "https://tixcraft.com/activity"
        html_content = get_dynamic_page_source(url, wait_time=10)
        if not html_content: return []
        soup = BeautifulSoup(html_content, 'html.parser')
        event_items = soup.select('div.activity-block')
        events = []
        for item in event_items:
            link = item.select_one('a.activity-link')
            if not link: continue
            title = item.select_one('.activity-name').text.strip()
            img_elem = item.select_one('.activity-thumbnail img')
            image = urljoin(url, img_elem.get('src') or img_elem.get('data-src', '')) if img_elem else ''
            full_url = urljoin("https://tixcraft.com/", link['href'])
            if title:
                events.append({'title': title, 'url': full_url, 'start_time': '詳見內文', 'platform': '拓元', 'image': image})
        print(f"成功解析出 {len(events)} 筆 拓元 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 拓元 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

# --- 年代 (類似宽宏) ---
def fetch_ticket_events(session):
    print("--- 開始從 年代售票 抓取活動 ---")
    base_url = "https://www.ticket.com.tw/"
    category_map = {
        "音樂會/演唱會": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=1",
        "展覽/博覽": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=3",
        "戲劇表演": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=2",
        "親子活動": "https://www.ticket.com.tw/application/UTK01/UTK0101_.aspx?CATEGORY=4",
    }
    events = generic_category_fetcher(session, "年代", category_map, base_url, 
                                      item_selector='ul#product_list li', 
                                      title_selector='div.product_name a', 
                                      image_selector='div.product_img img.lazy')
    print(f"成功解析出 {len(events)} 筆 年代 活動。")
    return events

# --- Event GO (移除verify, 修正選擇器) ---
def fetch_eventgo_events(session):
    print("--- 開始從 Event GO 抓取活動 ---")
    base_url = "https://eventgo.bnextmedia.com.tw/"
    try:
        url = base_url + "event/list"
        scraper = create_session(use_cloudscraper=True)
        response = scraper.get(url, timeout=20)  # 移除verify
        response.raise_for_status()
        html_content = response.text
        print(f"  成功下載 Event GO 頁面，長度: {len(html_content)} bytes")
        soup = BeautifulSoup(html_content, 'html.parser')
        event_items = soup.select('div.event-card, li.event')  # 推測選擇器
        events, seen_urls = [], set()
        for item in event_items:
            link = item.select_one('a[href*="event/detail"]')
            if not link: continue
            href = link.get('href')
            full_url = urljoin(base_url, href)
            if full_url in seen_urls or 'event/detail' not in full_url: continue
            title_elem = item.select_one('h3.event-title, .event-title')
            title = title_elem.text.strip() if title_elem else link.text.strip()
            if not title or len(title) <= 3: continue
            img_elem = item.select_one('img.event-img, img')
            image = urljoin(base_url, img_elem.get('src') or img_elem.get('data-src', '')) if img_elem else ''
            events.append({'title': title, 'url': full_url, 'start_time': '詳見內文', 'platform': 'Event GO', 'image': image})
            seen_urls.add(full_url)
        print(f"成功解析出 {len(events)} 筆 Event GO 活動。")
        return events
    except Exception as e:
        print(f"錯誤：抓取 Event GO 時發生嚴重錯誤: {e}"); traceback.print_exc(); return []

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
        fetch_kham_events,
        fetch_udn_events,
        fetch_ibon_events,
        fetch_kktix_events,
        fetch_tixcraft_events,
        fetch_ticket_events,
        fetch_eventgo_events,
    ]
    
    for task_func in scraper_tasks:
        all_events.extend(task_func(session))

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
