# scraper.py
import os
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup # <--- 新增

# Render 會自動將資料庫連線 URL 注入到環境變數中
DATABASE_URL = os.environ.get('DATABASE_URL')
KKTIX_API_URL = "https://kktix.com/g/events.json?order=updated_at_desc&page=1"

def setup_database(engine):
    """建立資料表 (如果不存在的話)"""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                url VARCHAR(255) UNIQUE,
                start_time VARCHAR(50),
                platform VARCHAR(50),
                image VARCHAR(255)
            );
        """))
        conn.commit()

def fetch_kktix_events():
    # ... (這個函式的內容完全不變，請從舊版程式碼複製過來) ...
    """從 KKTIX API 獲取活動資料"""
    events_data = []
    print("開始從 KKTIX 抓取活動...")
    try:
        response = requests.get(KKTIX_API_URL, timeout=15)
        if response.status_code == 200:
            data = response.json()
            raw_events = data.get('entry', [])
            
            for event in raw_events:
                try:
                    start_time = datetime.fromisoformat(event.get('start', '').replace('Z', '+00:00'))
                    formatted_time = start_time.strftime('%Y-%m-%d %H:%M')
                except (ValueError, TypeError):
                    formatted_time = "時間未定"

                events_data.append({
                    'title': event.get('title', '標題未知'),
                    'url': event.get('url', '#'),
                    'start_time': formatted_time,
                    'platform': 'KKTIX',
                    'image': event.get('img') or 'https://via.placeholder.com/300x200?text=No+Image'
                })
            print(f"成功抓取到 {len(events_data)} 筆 KKTIX 活動。")
            return events_data
        else:
            print(f"錯誤：KKTIX API 回應狀態碼 {response.status_code}")
            return []
            
    except requests.RequestException as e:
        print(f"錯誤：請求 KKTIX API 時發生網路錯誤: {e}")
        return []
        
def fetch_tixcraft_events():
    """從拓元網站抓取活動資料"""
    print("開始從拓元 tixcraft 抓取活動...")
    events_data = []
    try:
        url = "https://tixcraft.com/activity"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() # 如果請求失敗就拋出異常

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # *** 以下的 class name 是範例，需要你實際去拓元網站用 "開發者工具" 查看並可能需要調整 ***
        event_items = soup.find_all('div', class_='event-list-item') 

        for item in event_items:
            title_tag = item.find('h4', class_='event-name')
            url_tag = item.find('a', class_='event-image-link')
            date_tag = item.find('p', class_='event-date')
            img_tag = item.find('img')

            if title_tag and url_tag and date_tag and img_tag:
                events_data.append({
                    'title': title_tag.text.strip(),
                    'url': 'https://tixcraft.com' + url_tag['href'],
                    'start_time': date_tag.text.strip(), # 拓元的時間格式比較亂，先簡單處理
                    'platform': '拓元',
                    'image': img_tag['src']
                })
        
        print(f"成功抓取到 {len(events_data)} 筆拓元活動。")
        return events_data

    except requests.RequestException as e:
        print(f"錯誤：請求拓元網站時發生網路錯誤: {e}")
        return []
    except Exception as e:
        print(f"錯誤：解析拓元網頁時發生未知錯誤: {e}")
        return []

def save_data_to_db(engine, events):
    """將資料儲存到資料庫"""
    if not events:
        print("沒有活動資料可以儲存。")
        return

    with engine.connect() as conn:
        # 為了簡單起見，每次都清空舊資料再插入新的
        conn.execute(text("TRUNCATE TABLE events RESTART IDENTITY;"))
        print("清空舊資料完成。")
        
        # 插入新資料
        for event in events:
            stmt = text("""
                INSERT INTO events (title, url, start_time, platform, image)
                VALUES (:title, :url, :start_time, :platform, :image)
                ON CONFLICT (url) DO NOTHING;
            """)
            conn.execute(stmt, event)
        
        conn.commit()
        print(f"成功將 {len(events)} 筆新資料寫入資料庫。")

if __name__ == "__main__":
    if not DATABASE_URL:
        raise Exception("環境變數 DATABASE_URL 未設定")

    db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(db_url_for_sqlalchemy)
    
    setup_database(engine)
    
    all_events = []
    
    # 執行 KKTIX 爬蟲
    kktix_events = fetch_kktix_events()
    if kktix_events:
        all_events.extend(kktix_events)
        
    # 執行拓元爬蟲
    tixcraft_events = fetch_tixcraft_events()
    if tixcraft_events:
        all_events.extend(tixcraft_events)

    if all_events:
        # 將所有平台的資料一起存入資料庫
        save_data_to_db(engine, all_events)
    else:
        print("沒有抓取到任何活動，不更新資料檔案。")
