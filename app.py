# app.py
import os
from flask import Flask, render_template
from sqlalchemy import create_engine, text
from datetime import datetime
import pytz

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise Exception("環境變數 DATABASE_URL 未設定")

db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
engine = create_engine(db_url_for_sqlalchemy)

def get_platform_status():
    """計算每個平台的活動總數"""
    platform_names = ["KKTIX", "拓元", "寬宏", "iBon", "UDN", "OPENTIX", "Event Go"]  # 新增Event Go
    status = {name: 0 for name in platform_names}
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT platform, COUNT(*) as count FROM events GROUP BY platform;"))
            for row in result:
                if row.platform in status:
                    status[row.platform] = row.count
    except Exception as e:
        print(f"計算平台狀態時出錯: {e}")
    return status

def get_current_taipei_time():
    """取得格式化後的台北時間"""
    taipei_tz = pytz.timezone('Asia/Taipei')
    now_taipei = datetime.now(taipei_tz)
    return {
        "date": now_taipei.strftime("%Y/%m/%d"),
        "time": now_taipei.strftime("%p %I:%M:%S").replace("AM", "上午").replace("PM", "下午")
    }

@app.route('/')
def home():
    """主頁面，顯示最新的 50 筆活動"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM events ORDER BY id DESC LIMIT 50;"))
            events = [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"讀取主頁活動時出錯: {e}")
        events = []
    
    platform_status = get_platform_status()
    current_time = get_current_taipei_time()

    return render_template(
        'index.html', 
        events=events, 
        platform_status=platform_status,
        current_date=current_time["date"],
        last_update=current_time["time"],
        page_title="所有最新活動"
    )

@app.route('/platform/<platform_name>')
def platform_page(platform_name):
    """平台專屬頁面，顯示該平台所有活動"""
    try:
        with engine.connect() as conn:
            stmt = text("SELECT * FROM events WHERE platform = :platform ORDER BY id DESC;")
            result = conn.execute(stmt, {"platform": platform_name})
            events = [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"讀取平台 {platform_name} 活動時出錯: {e}")
        events = []
    
    platform_status = get_platform_status()
    current_time = get_current_taipei_time()

    return render_template(
        'index.html', 
        events=events, 
        platform_status=platform_status,
        current_date=current_time["date"],
        last_update=current_time["time"],
        page_title=f"{platform_name} 所有活動"
    )

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=81)
