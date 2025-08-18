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

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRAPER_VERSION = "v11.0"
DATABASE_URL = os.environ.get('DATABASE_URL')

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
]

def create_robust_session():
    """仿照show_news_v12.py的成功經驗創建session"""
    session = requests.Session()
    
    # 更保守的重試策略
    retry = Retry(
        total=2, 
        read=2, 
        connect=2, 
        backoff_factor=0.6, 
        status_forcelist=(429, 500, 502, 503, 504)  # 移除403, 401避免過度重試
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # 更真實的headers，參考show_news_v12.py
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
    
    session.verify = False
    return session

def print_page_debug_info(html_content, platform_name, max_chars=2000):
    """打印頁面調試資訊"""
    if not html_content:
        print(f"[DEBUG] {platform_name}: 無HTML內容")
        return
    
    print(f"[DEBUG] {platform_name}: 頁面長度 {len(html_content)} bytes")
    
    # 打印頁面開頭
    print(f"[DEBUG] {platform_name}: 頁面開頭內容:")
    print("=" * 50)
    print(html_content[:max_chars])
    print("=" * 50)
    
    # 檢查是否包含常見的錯誤頁面標識
    error_indicators = ['404', '403', 'error', 'blocked', 'captcha', '驗證', '錯誤', 'forbidden']
    found_errors = [indicator for indicator in error_indicators if indicator.lower() in html_content.lower()]
    if found_errors:
        print(f"[DEBUG] {platform_name}: 發現錯誤指標: {found_errors}")
    
    # 檢查頁面結構
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.find('title')
    if title:
        print(f"[DEBUG] {platform_name}: 頁面標題: {title.get_text(strip=True)}")
    
    # 檢查常見元素數量
    common_tags = ['div', 'a', 'img', 'li', 'span']
    for tag in common_tags:
        count = len(soup.find_all(tag))
        print(f"[DEBUG] {platform_name}: {tag} 元素數量: {count}")

def is_blocked_page(html_content, platform_name):
    """更智能的反爬蟲檢測"""
    if not html_content or len(html_content) < 500:
        print(f"[DEBUG] {platform_name}: 頁面過短，可能被攔截")
        return True
    
    # 檢查明確的阻擋關鍵字（更嚴格的判斷）
    strong_block_indicators = [
        'blocked', 'captcha', 'verification required', '人機驗證',
        'access denied', '拒絕訪問', 'robot', 'cloudflare'
    ]
    
    for indicator in strong_block_indicators:
        if indicator.lower() in html_content.lower():
            print(f"[DEBUG] {platform_name}: 檢測到強阻擋指標: {indicator}")
            return True
    
    return False

def safe_request(url, session, platform_name="Unknown", max_attempts=2):
    """安全請求函數，增強debug資訊"""
    print(f"[DEBUG] 正在請求 {platform_name}: {url}")
    
    for attempt in range(max_attempts):
        try:
            # 每次請求前稍作延遲
            time.sleep(random.uniform(1, 3))
            
            # 設置這次請求的特殊headers
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Referer': f"https://{url.split('/')[2]}/",
                'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8'
            }
            
            print(f"[DEBUG] {platform_name}: 第{attempt+1}次嘗試")
            resp = session.get(url, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            print(f"[DEBUG] {platform_name}: HTTP狀態 {resp.status_code}")
            print(f"[DEBUG] {platform_name}: 回應headers: {dict(resp.headers)}")
            
            # 詳細debug頁面內容
            print_page_debug_info(resp.text, platform_name)
            
            # 檢查是否被阻擋
            if is_blocked_page(resp.text, platform_name):
                print(f"[DEBUG] {platform_name}: 檢測到頁面被阻擋，嘗試下一種方法")
                continue
            
            return resp.text, None
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] {platform_name}: 第{attempt+1}次失敗 - {error_msg}")
            
            if '403' in error_msg:
                print(f"[DEBUG] {platform_name}: 403錯誤，延長休息時間")
                time.sleep(random.uniform(5, 10))
            elif 'timeout' in error_msg.lower():
                print(f"[DEBUG] {platform_name}: 超時錯誤，延長下次timeout")
            
    return None, "所有請求嘗試都失敗"

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

def extract_events_with_debug(soup, selectors, platform_name):
    """使用多個selector嘗試並debug"""
    events = []
    
    for selector_name, selector in selectors.items():
        print(f"[DEBUG] {platform_name}: 嘗試selector '{selector_name}': {selector}")
        items = soup.select(selector)
        print(f"[DEBUG] {platform_name}: selector '{selector_name}' 找到 {len(items)} 個元素")
        
        if items:
            print(f"[DEBUG] {platform_name}: 使用 '{selector_name}' 解析活動")
            for i, item in enumerate(items[:5]):  # 只處理前5個作為測試
                print(f"[DEBUG] {platform_name}: 處理第{i+1}個元素: {str(item)[:200]}...")
                
                # 嘗試提取連結和標題
                link_elem = item.select_one('a')
                if link_elem:
                    href = link_elem.get('href')
                    title = link_elem.get_text(strip=True)
                    print(f"[DEBUG] {platform_name}: 找到連結: {href}, 標題: {title}")
                    
                    if href and title and len(title) > 3:
                        events.append({
                            'element': item,
                            'link': href, 
                            'title': title,
                            'selector': selector_name
                        })
                        
            if events:
                print(f"[DEBUG] {platform_name}: 成功使用 '{selector_name}' 提取了 {len(events)} 個活動")
                break
    
    return events

def fetch_opentix_events(sess):
    print("--- 開始從 OPENTIX 抓取活動 ---")
    url = "https://www.opentix.life"
    
    try:
        html_content, error = safe_request(url, sess, platform_name="OPENTIX")
        if not html_content:
            print(f"[DEBUG] OPENTIX 獲取頁面失敗: {error}")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 多種selector策略
        selectors = {
            'event_links': 'a[href*="/event/"]',
            'alternative_1': 'div.event a',
            'alternative_2': '.event-item a',
            'alternative_3': 'div[class*="event"] a',
            'generic': 'a[href*="event"]'
        }
        
        raw_events = extract_events_with_debug(soup, selectors, "OPENTIX")
        
        events = []
        for raw_event in raw_events:
            href = raw_event['link']
            if not href.startswith('/event/'):
                continue
                
            full_url = urljoin(url, href)
            title = raw_event['title']
            
            # 嘗試找圖片
            img_elem = raw_event['element'].find('img')
            image = urljoin(url, img_elem['src']) if img_elem and img_elem.get('src') else ''
            
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'OPENTIX',
                'image': image
            })
            
            print(f"[DEBUG] OPENTIX: 添加活動: {title}")
        
        print(f"[DEBUG] OPENTIX: 成功解析出 {len(events)} 筆活動")
        return events
        
    except Exception as e:
        print(f"[DEBUG] OPENTIX 爬取異常: {e}")
        print(traceback.format_exc())
        return []

def fetch_kham_events(sess):
    print("--- 開始從 寬宏 抓取活動 ---")
    result = []
    category_map = {
        "音樂會/演唱會": "https://kham.com.tw/application/UTK01/UTK0101_06.aspx?TYPE=1&CATEGORY=205",
    }
    base_url = "https://kham.com.tw/"
    
    # 只測試一個分類避免過多輸出
    for category_name, category_url in list(category_map.items())[:1]:
        try:
            html_content, error = safe_request(category_url, sess, platform_name=f"寬宏-{category_name}")
            if not html_content:
                print(f"[DEBUG] 寬宏-{category_name} 獲取頁面失敗: {error}")
                continue
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            selectors = {
                'product_list': 'ul#product_list li',
                'alternative_1': '.product-item',
                'alternative_2': 'div.product',
                'alternative_3': 'li[class*="product"]',
                'generic': 'li a'
            }
            
            raw_events = extract_events_with_debug(soup, selectors, f"寬宏-{category_name}")
            
            for raw_event in raw_events:
                title = raw_event['title']
                link = raw_event['link']
                full_url = urljoin(base_url, link)
                
                # 查找圖片
                img_elem = raw_event['element'].select_one('img')
                image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
                
                result.append({
                    'title': title,
                    'url': full_url,
                    'start_time': '詳見內文',
                    'platform': '寬宏',
                    'image': image
                })
                
                print(f"[DEBUG] 寬宏-{category_name}: 添加活動: {title}")
            
        except Exception as e:
            print(f"[DEBUG] 寬宏-{category_name}抓取異常: {e}")
            print(traceback.format_exc())
            
        time.sleep(random.uniform(2, 4))
    
    print(f"[DEBUG] 寬宏: 總共解析出 {len(result)} 筆活動")
    return result

# 簡化其他平台函數，重點在debug
def fetch_kktix_events(sess):
    print("--- 開始從 KKTIX 抓取活動 ---")
    url = "https://kktix.com/events"
    
    try:
        html_content, error = safe_request(url, sess, platform_name="KKTIX")
        if not html_content:
            print(f"[DEBUG] KKTIX 獲取頁面失敗: {error}")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        selectors = {
            'event_list': 'ul.event-list li',
            'alternative_1': 'div.event',
            'alternative_2': '.event-item',
            'generic': 'li a, div a'
        }
        
        raw_events = extract_events_with_debug(soup, selectors, "KKTIX")
        
        events = []
        for raw_event in raw_events[:10]:  # 限制處理數量
            title = raw_event['title']
            link = raw_event['link']
            full_url = urljoin("https://kktix.com/", link)
            
            img_elem = raw_event['element'].select_one('img')
            image = img_elem.get('data-src') or img_elem.get('src', '') if img_elem else ''
            
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'KKTIX',
                'image': image
            })
            
            print(f"[DEBUG] KKTIX: 添加活動: {title}")
        
        print(f"[DEBUG] KKTIX: 成功解析出 {len(events)} 筆活動")
        return events
        
    except Exception as e:
        print(f"[DEBUG] KKTIX 爬取異常: {e}")
        print(traceback.format_exc())
        return []

def fetch_eventgo_events(sess):
    print("--- 開始從 Event GO 抓取活動 ---")
    url = "https://eventgo.bnextmedia.com.tw/event/list"
    base_url = "https://eventgo.bnextmedia.com.tw/"
    
    try:
        html_content, error = safe_request(url, sess, platform_name="Event GO")
        if not html_content:
            print(f"[DEBUG] Event GO 獲取頁面失敗: {error}")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        selectors = {
            'event_cards': 'div.event-card, li.event-item',
            'alternative_1': 'div.event',
            'alternative_2': '.event-list li',
            'generic': 'a[href*="event"]'
        }
        
        raw_events = extract_events_with_debug(soup, selectors, "Event GO")
        
        events = []
        for raw_event in raw_events:
            href = raw_event['link']
            if 'event/detail' not in href:
                continue
                
            title = raw_event['title']
            full_url = urljoin(base_url, href)
            
            img_elem = raw_event['element'].select_one('img')
            image = urljoin(base_url, img_elem.get('data-src') or img_elem.get('src', '')) if img_elem else ''
            
            events.append({
                'title': title,
                'url': full_url,
                'start_time': '詳見內文',
                'platform': 'Event GO',
                'image': image
            })
            
            print(f"[DEBUG] Event GO: 添加活動: {title}")
        
        print(f"[DEBUG] Event GO: 成功解析出 {len(events)} 筆活動")
        return events
        
    except Exception as e:
        print(f"[DEBUG] Event GO 爬取異常: {e}")
        print(traceback.format_exc())
        return []

if __name__ == "__main__":
    print(f"===== 開始執行票券爬蟲 {SCRAPER_VERSION} =====")
    if not DATABASE_URL:
        raise Exception("環境變數 DATABASE_URL 未設定")
        
    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    setup_database(engine)
    
    sess = create_robust_session()
    all_events = []
    
    # 先測試幾個主要平台
    test_platforms = [
        ('OPENTIX', fetch_opentix_events),
        ('寬宏', fetch_kham_events),
        ('KKTIX', fetch_kktix_events),
        ('Event GO', fetch_eventgo_events),
    ]
    
    for platform_name, fetcher_func in test_platforms:
        try:
            print(f"\n[DEBUG] ========== 開始處理平台: {platform_name} ==========")
            events = fetcher_func(sess)
            all_events.extend(events)
            print(f"[DEBUG] {platform_name} 完成，獲得 {len(events)} 筆活動")
            
            # 展示獲得的活動
            for i, event in enumerate(events[:3]):  # 只顯示前3個
                print(f"[DEBUG] {platform_name} 活動{i+1}: {event['title']} -> {event['url']}")
                
        except Exception as e:
            print(f"[DEBUG] {platform_name} 處理異常: {e}")
            print(traceback.format_exc())
        
        print(f"[DEBUG] ========== {platform_name} 處理完畢 ==========\n")
        time.sleep(random.uniform(3, 6))  # 平台間較長休息
    
    # 去重處理
    final_events, processed_urls = [], set()
    for event in all_events:
        if event.get('url') and event['url'] not in processed_urls:
            final_events.append(event)
            processed_urls.add(event['url'])
    
    print(f"\n[DEBUG] 總計抓取到 {len(final_events)} 筆不重複的活動。")
    
    # 展示最終結果
    for i, event in enumerate(final_events):
        print(f"[DEBUG] 最終活動{i+1}: [{event['platform']}] {event['title']}")
    
    if final_events:
        save_data_to_db(engine, final_events)
        print(f"[DEBUG] 已將 {len(final_events)} 筆活動寫入資料庫")
    else:
        print("[DEBUG] 沒有活動資料，跳過資料庫寫入")
    
    print(f"===== 票券爬蟲 {SCRAPER_VERSION} 執行完畢 =====")
