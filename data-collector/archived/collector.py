import schedule
import time
from market_data import get_stock_price
from db_handler import get_db_connection
from datetime import datetime

def collect_realtime_data():
    now = datetime.now()  # 로컬 시간 사용
    print(f"[{now}] 데이터 수집 시작...")
    
    stock_code = "005930"
    data = get_stock_price(stock_code)
    
    if data:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO realtime_prices (stock_code, current_price, volume, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (stock_code, data['stck_prpr'], data.get('acml_vol', 0), now))
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ {stock_code} 저장")

schedule.every(10).seconds.do(collect_realtime_data)

print("🚀 스케줄러 시작...")
while True:
    schedule.run_pending()
    time.sleep(1)
