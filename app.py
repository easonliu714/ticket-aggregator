# app.py
import os
from flask import Flask, render_template, request
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
    platform_names = ["KKTIX", "拓元", "寬宏", "iBon", "UDN", "OPENTIX", "年代", "Event GO"]
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

def query_events(limit=50, search='', platform='', event_type='', sort='id DESC'):
    """通用查詢事件，支持搜尋/篩選"""
    base_sql = "SELECT * FROM events WHERE 1=1"
    params = {}
    
    if search:
        base_sql += " AND title ILIKE :search"
        params['search'] = f"%{search}%"
    if platform:
        base_sql += " AND platform = :platform"
        params['platform'] = platform
    if event_type:
        base_sql += " AND event_type ILIKE :type"
        params['type'] = f"%{event_type}%"
    
    base_sql += f" ORDER BY {sort} LIMIT {limit};"
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(base_sql), params)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"查詢事件時出錯: {e}")
        return []

@app.route('/')
def home():
    search = request.args.get('search', '')
    event_type = request.args.get('type', '')
    plat = request.args.get('platform', '')
    sort = request.args.get('sort', 'id DESC')
    
    events = query_events(50, search, plat, event_type, sort)
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
    search = request.args.get('search', '')
    event_type = request.args.get('type', '')
    sort = request.args.get('sort', 'id DESC')
    
    events = query_events(search=search, platform=platform_name, event_type=event_type, sort=sort)
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
