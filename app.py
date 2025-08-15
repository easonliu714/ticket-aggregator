# app.py
import os
from flask import Flask, render_template
from sqlalchemy import create_engine, text
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise Exception("環境變數 DATABASE_URL 未設定")

db_url_for_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
engine = create_engine(db_url_for_sqlalchemy)

def load_events_from_db():
    """從資料庫讀取活動資料"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM events ORDER BY id DESC;"))
            events = [dict(row._mapping) for row in result]
            return events
    except Exception as e:
        print(f"讀取資料庫時發生錯誤: {e}")
        return []

@app.route('/')
def home():
    events = load_events_from_db()

    # 動態計算平台狀態
    platform_names = ["KKTIX", "拓元", "寬宏", "iBon", "UDN", "OPENTIX"]
    platform_status = {name: 0 for name in platform_names}
    for event in events:
        if event['platform'] in platform_status:
            platform_status[event['platform']] += 1

    now = datetime.now().strftime("%Y/%m/%d")
    last_update = datetime.now().strftime("%p %I:%M:%S").replace("AM", "上午").replace("PM", "下午")

    return render_template(
        'index.html', 
        events=events, 
        platform_status=platform_status,
        current_date=now,
        last_update=last_update
    )

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=81)
