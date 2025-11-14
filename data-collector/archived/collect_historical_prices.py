from market_data import get_token
import requests
import os
from db_handler import get_db_connection
from datetime import datetime, timedelta
import time

APP_KEY = os.getenv('KIS_APP_KEY')
BASE_URL = "https://openapi.koreainvestment.com:9443"

def get_daily_price(stock_code, start_date, end_date):
    """일별 시세 조회"""
    token = get_token()
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": os.getenv('KIS_APP_SECRET'),
        "tr_id": "FHKST01010400"
    }
    
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": stock_code,
        "fid_org_adj_prc": "0",
        "fid_period_div_code": "D"
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json() if response.status_code == 200 else None

def save_daily_prices(stock_code):
    """과거 데이터 저장"""
    print(f"📈 {stock_code} 수집 중...")
    data = get_daily_price(stock_code, '20200101', datetime.now().strftime('%Y%m%d'))
    
    if not data or 'output' not in data:
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    for item in data['output']:
        cur.execute("""
            INSERT INTO daily_prices 
            (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, trade_date) DO NOTHING
        """, (
            stock_code,
            item['stck_bsop_date'],
            item['stck_oprc'],
            item['stck_hgpr'],
            item['stck_lwpr'],
            item['stck_clpr'],
            item['acml_vol']
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {stock_code} 완료")

if __name__ == "__main__":
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT stock_code FROM stocks WHERE asset_type='STOCK' LIMIT 10")  # 테스트: 10개만
    stocks = cur.fetchall()
    cur.close()
    conn.close()
    
    for (code,) in stocks:
        save_daily_prices(code)
        time.sleep(1)  # API 제한 고려