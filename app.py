# app.py
from flask import Flask, render_template
import json
import os
from datetime import datetime

app = Flask(__name__)

# 與 scraper.py 使用相同的路徑讀取資料
DATA_FILE = '/data/events.json'

def load_events_from_file():
    """從 JSON 檔案讀取活動資料"""
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # 如果檔案不存在或格式錯誤，返回空列表
        return []

@app.route('/')
def home():
    events = load_events_from_file()

    # 模擬平台狀態數據
    platform_status = {
        "KKTIX": len([e for e in events if e['platform'] == 'KKTIX']),
        "拓元": 0, "寬宏": 0, "iBon": 1, "UDN": 0, "OPENTIX": 1255
    }
    
    now = datetime.now().strftime("%Y/%m/%d")
    
    # 取得檔案的最後修改時間
    try:
        last_update_timestamp = os.path.getmtime(DATA_FILE)
        last_update_dt = datetime.fromtimestamp(last_update_timestamp)
        last_update = last_update_dt.strftime("%p %I:%M:%S").replace("AM", "上午").replace("PM", "下午")
    except FileNotFoundError:
        last_update = "N/A"

    return render_template(
        'index.html', 
        events=events, 
        platform_status=platform_status,
        current_date=now,
        last_update=last_update
    )

if __name__ == "__main__":
    # Render 會使用 Gunicorn 啟動，這個區塊在本機測試時才會執行
    app.run(host='0.0.0.0', port=81)
