# scraper.py
import requests
import json
import os
from datetime import datetime

# Render 的 Persistent Disk 會掛載在 /data/ 目錄下
# 我們將資料檔案存在這個目錄中
DATA_DIR = '/data'
DATA_FILE = os.path.join(DATA_DIR, 'events.json')
KKTIX_API_URL = "https://kktix.com/g/events.json?order=updated_at_desc&page=1"

def fetch_kktix_events():
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
            return None
            
    except requests.RequestException as e:
        print(f"錯誤：請求 KKTIX API 時發生網路錯誤: {e}")
        return None

def save_data(data):
    """將資料寫入 JSON 檔案"""
    print(f"準備將資料寫入到 {DATA_FILE}...")
    # 確保 /data 目錄存在
    os.makedirs(DATA_DIR, exist_ok=True)
    
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print("資料寫入成功！")

if __name__ == "__main__":
    # 這裡可以加入更多平台的爬蟲函式
    all_events = []
    
    kktix_events = fetch_kktix_events()
    if kktix_events:
        all_events.extend(kktix_events)
    
    # 未來加入其他平台...
    # topx_events = fetch_topx_events()
    # if topx_events:
    #     all_events.extend(topx_events)

    if all_events:
        save_data(all_events)
    else:
        print("沒有抓取到任何活動，不更新資料檔案。")
