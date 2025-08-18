import os
import time
import random
import urllib3
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import cloudscraper

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRAPER_VERSION = "v10.0"
DATABASE_URL = os.environ.get('DATABASE_URL')

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
]

def create_session():
    session = requests.Session()
    retry = Retry(
        total=3, 
        read=3, 
        connect=3, 
        backoff_factor=2, 
        status_forcelist=(429, 500, 502, 503, 504, 403, 401)
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
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
    # 關閉SSL驗證以避免證書問題
    session.verify = False
    return session

def create_cloudscraper():
    return cloudscraper.create_scraper(
        browser={'custom': random.choice(USER_AGENTS)},
        delay=random.uniform(1, 3)
    )

def safe_request(url, session=None, use_cloudscraper=False, platform_name="Unknown"):
    """安全請求函數，包含多種fallback機制"""
    print(f"正在請求 {platform_name}: {url}")
    
    methods = []
    
    # 方法1：使用提供的session
    if session and not use_cloudscraper:
        methods.append(('regular_session', session))
    
    # 方法2：使用cloudscraper
    try:
        scraper = create_cloudscraper()
        methods.append(('cloudscraper', scraper))
    except:
        print(f"{platform_name}: cloudscraper 初始化失敗")
    
    # 方法3：創建新的session
    if not session:
        new_session = create_session()
        methods.append(('new_session', new_session))
    
    for method_name, req_session in methods:
        try:
            print(f"{platform_name}: 嘗試使用 {method_name}")
            
            # 隨機延遲
            time.sleep(random.uniform(1, 3))
            
            # 設置超時和headers
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Referer': f"https://{url.split('/')[2]}/",
                'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8'
            }
            
            resp = req_session.get(url, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            print(f"{platform_name}: 成功獲取頁面，長度: {len(resp.text)} bytes")
            
            # 檢查是否被反爬蟲攔截
            if len(resp.text) < 1000:
                print(f"{platform_name}: 頁面長度過短，可能被攔截")
                continue
                
            if any(blocked in resp.text.lower() for blocked in ['blocked', 'captcha', 'verification', '驗證']):
                print(f"{platform_name}: 檢測到反爬蟲機制")
                continue
                
            return resp.text, None
            
        except Exception as e:
            error_msg = str(e)
            print(f"{platform_name}: {method_name} 失敗 - {error_msg}")
            
            # 特殊處理某些錯誤
            if '403' in error_msg:
                print(f"{platform_name}: 403錯誤，可能被反爬蟲攔截")
                time.sleep(random.uniform(5, 10))
            elif 'ssl' in error_msg.lower() or 'certificate' in error_msg.lower():
                print(f"{platform_name}: SSL錯誤，已嘗試跳過驗證")
            
            continue
    
    return None, f"所有請求方法都失敗"

def debug_page_content(html_content, platform_name, selector):
    """調試頁面內容"""
    if not html_content:
        print(f"{platform_name}: 無HTML內容")
        return
        
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 保存部分HTML用於調試
    debug_file = f"debug_{platform_name.lower()}.html"
    try:
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(html_content[:50000])  # 只保存前50KB
        print(f"{platform_name}: 已保存調試文件 {debug_file}")
    except:
        pass
    
    # 檢查selector
    items = soup.select(selector)
    print(f"{platform_name}: selector '{selector}' 找到 {len(items)} 個元素")
    
    # 如果沒找到，嘗試一些常見的選擇器
    if len(items) == 0:
        common_selectors = ['div', 'li', 'a', '.event', '.activity', '.product']
        print(f"{platform_name}: 嘗試查找常見選擇器...")
        for sel in common_selectors:
            found = soup.select(sel)
            if found:
                print(f"{platform_name}: 找到 {len(found)} 個 '{sel}' 元素")

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
        html_content, error = safe_request(url, sess, platform_name="OPENTIX")
        if not html_content:
            print(f"OPENTIX 獲取頁面失敗: {error}")
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = soup.select('a[href*="/event/"]')
        print(f"OPENTIX: 找到 {len(event_links)} 個活動連結")
        
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
            events.append({
                'title': title_text,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'OPENTIX',
                'image': image
            })
            seen_urls.add(href)
        print(f"成功解析出 {len(events)} 筆 OPENTIX 活動。")
        return events
    except Exception as e:
        print(f"OPENTIX 爬取異常: {e}")
        print(traceback.format_exc())
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
            html_content, error = safe_request(category_url, sess, platform_name=f"寬宏-{category_name}")
            if not html_content:
                print(f"寬宏-{category_name} 獲取頁面失敗: {error}")
                continue
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 調試頁面內容
            debug_page_content(html_content, f"寬宏-{category_name}", 'ul#product_list li')
            
            items = soup.select('ul#product_list li')
            if not items:
                # 嘗試其他可能的選擇器
                alternative_selectors = [
                    'li.product-item', 'div.product', '.event-item', 
                    'div.activity', 'li', 'div.item'
                ]
                for alt_sel in alternative_selectors:
                    items = soup.select(alt_sel)
                    if items:
                        print(f"寬宏-{category_name}: 使用替代選擇器 '{alt_sel}' 找到 {len(items)} 個項目")
                        break
            
            for item in items:
                t_link = item.select_one('div.product_name a') or item.select_one('a')
                if not t_link:
                    continue
                title = t_link.text.strip()
                link = t_link.get('href')
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                img_elem = item.select_one('div.product_img img.lazy') or item.select_one('img')
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
            print(f"寬宏-{category_name}抓取異常: {e}")
            print(traceback.format_exc())
        time.sleep(random.uniform(2, 4))
    
    print(f"成功解析出 {len(result)} 筆 寬宏 活動。")
    return result

def fetch_kktix_events(sess):
    print("--- 開始從 KKTIX 抓取活動 ---")
    url = "https://kktix.com/events"
    try:
        # KKTIX有嚴格的反爬蟲，優先使用cloudscraper
        html_content, error = safe_request(url, sess, use_cloudscraper=True, platform_name="KKTIX")
        if not html_content:
            print(f"KKTIX 獲取頁面失敗: {error}")
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        debug_page_content(html_content, "KKTIX", 'ul.event-list li')
        
        event_items = soup.select('ul.event-list li')
        if not event_items:
            # 嘗試其他選擇器
            event_items = soup.select('div.event') or soup.select('li') or soup.select('div.activity')
            
        events, seen_urls = [], set()
        for item in event_items:
            link = item.select_one('a.event-link') or item.select_one('a')
            if not link:
                continue
            href = link.get('href')
            if not href:
                continue
            full_url = urljoin("https://kktix.com/", href)
            if full_url in seen_urls:
                continue
            title_elem = item.select_one('.event-title') or link
            title = safe_get_text(title_elem, link.text if link else '')
            img_elem = item.select_one('img.event-image') or item.select_one('img')
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
        print(f"KKTIX 爬取異常: {e}")
        print(traceback.format_exc())
        return []

def fetch_tixcraft_events(sess):
    print("--- 開始從 拓元 抓取活動 ---")
    url = "https://tixcraft.com/activity"
    try:
        # 拓元也有反爬蟲，使用cloudscraper
        html_content, error = safe_request(url, sess, use_cloudscraper=True, platform_name="拓元")
        if not html_content:
            print(f"拓元 獲取頁面失敗: {error}")
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        debug_page_content(html_content, "拓元", 'div.activity-block')
        
        event_items = soup.select('div.activity-block')
        if not event_items:
            event_items = soup.select('div.event') or soup.select('li') or soup.select('div.activity')
            
        events = []
        for item in event_items:
            link = item.select_one('a[href*="/activity/detail"]') or item.select_one('a')
            if not link:
                continue
            title_elem = item.select_one('.activity-name') or link
            title = title_elem.text.strip() if title_elem else link.text.strip()
            img_elem = item.select_one('.activity-thumbnail img') or item.select_one('img')
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
        print(f"拓元 爬取異常: {e}")
        print(traceback.format_exc())
        return []

# 其他平台函數類似修改...（UDN, iBon, 年代, Event GO）
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
            html_content, error = safe_request(category_url, sess, platform_name=f"UDN-{category_name}")
            if not html_content:
                print(f"UDN-{category_name} 獲取頁面失敗: {error}")
                continue
                
            soup = BeautifulSoup(html_content, 'html.parser')
            debug_page_content(html_content, f"UDN-{category_name}", 'ul#product_list li')
            
            items = soup.select('ul#product_list li')
            if not items:
                items = soup.select('li.product-item') or soup.select('div.product') or soup.select('li')
            
            for item in items:
                t_link = item.select_one('div.product_name a') or item.select_one('a')
                if not t_link:
                    continue
                title = t_link.text.strip()
                link = t_link.get('href')
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                img_elem = item.select_one('div.product_img img.lazy') or item.select_one('img')
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
            print(f"UDN-{category_name}抓取異常: {e}")
        time.sleep(random.uniform(1, 3))
    
    print(f"成功解析出 {len(result)} 筆 UDN 活動。")
    return result

def fetch_ibon_events(sess):
    print("--- 開始從 iBon 抓取活動 ---")
    url = "https://ticket.ibon.com.tw/Index/entertainment"
    try:
        html_content, error = safe_request(url, sess, platform_name="iBon")
        if not html_content:
            print(f"iBon 獲取頁面失敗: {error}")
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        debug_page_content(html_content, "iBon", 'div.ticket-item')
        
        event_items = soup.select('div.ticket-item')
        if not event_items:
            event_items = soup.select('div.event') or soup.select('li') or soup.select('div.activity')
        
        events = []
        for item in event_items:
            link = item.select_one('a[href*="/activity/detail"]') or item.select_one('a')
            if not link:
                continue
            title_elem = item.select_one('div.ticket-info div.ticket-title') or item.select_one('.title') or link
            title = title_elem.text.strip() if title_elem else ''
            img_elem = item.select_one('img')
            image = urljoin(url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            if not title or len(title) <= 3:
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
        print(f"iBon 爬取異常: {e}")
        print(traceback.format_exc())
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
            html_content, error = safe_request(category_url, sess, platform_name=f"年代-{category_name}")
            if not html_content:
                print(f"年代-{category_name} 獲取頁面失敗: {error}")
                continue
                
            soup = BeautifulSoup(html_content, 'html.parser')
            debug_page_content(html_content, f"年代-{category_name}", 'ul#product_list li')
            
            items = soup.select('ul#product_list li')
            if not items:
                items = soup.select('li.product-item') or soup.select('div.product') or soup.select('li')
            
            for item in items:
                t_link = item.select_one('div.product_name a') or item.select_one('a')
                if not t_link:
                    continue
                title = t_link.text.strip()
                link = t_link.get('href')
                if not link or not title or len(title) <= 3:
                    continue
                full_url = urljoin(base_url, link)
                img_elem = item.select_one('div.product_img img.lazy') or item.select_one('img')
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
            print(f"年代-{category_name}抓取異常: {e}")
        time.sleep(random.uniform(1, 3))
    
    print(f"成功解析出 {len(result)} 筆 年代 活動。")
    return result

def fetch_eventgo_events(sess):
    print("--- 開始從 Event GO 抓取活動 ---")
    url = "https://eventgo.bnextmedia.com.tw/event/list"
    base_url = "https://eventgo.bnextmedia.com.tw/"
    try:
        html_content, error = safe_request(url, sess, platform_name="Event GO")
        if not html_content:
            print(f"Event GO 獲取頁面失敗: {error}")
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        debug_page_content(html_content, "Event GO", 'div.event-card, li.event-item')
        
        event_items = soup.select('div.event-card, li.event-item')
        if not event_items:
            event_items = soup.select('div.event') or soup.select('li') or soup.select('div.activity')
        
        events = []
        for item in event_items:
            link = item.select_one('a[href*="event/detail"]') or item.select_one('a')
            if not link:
                continue
            href = link.get('href')
            if not href:
                continue
            full_url = urljoin(base_url, href)
            title_elem = item.select_one('h3.event-title, .event-title') or link
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
        print(f"Event GO 爬取異常: {e}")
        print(traceback.format_exc())
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
    
    # 執行各平台爬取
    fetchers = [
        ('OPENTIX', fetch_opentix_events),
        ('寬宏', fetch_kham_events),
        ('UDN', fetch_udn_events),
        ('iBon', fetch_ibon_events),
        ('KKTIX', fetch_kktix_events),
        ('拓元', fetch_tixcraft_events),
        ('年代', fetch_ticket_events),
        ('Event GO', fetch_eventgo_events),
    ]
    
    for platform_name, fetcher_func in fetchers:
        try:
            print(f"\n開始處理平台: {platform_name}")
            events = fetcher_func(sess)
            all_events.extend(events)
            print(f"{platform_name} 完成，獲得 {len(events)} 筆活動")
        except Exception as e:
            print(f"{platform_name} 處理異常: {e}")
            print(traceback.format_exc())
        
        # 平台間休息
        time.sleep(random.uniform(2, 5))
    
    # 去重處理
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
