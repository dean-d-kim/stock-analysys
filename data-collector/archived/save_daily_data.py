from db_handler import get_db_connection
from datetime import datetime, timedelta

def save_daily_data():
    """테스트용 일별 데이터 생성"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    base_price = 107500
    for i in range(30, 0, -1):
        date = datetime.now() - timedelta(days=i)
        price = base_price + (i % 5 - 2) * 1000
        
        cur.execute("""
            INSERT INTO daily_prices 
            (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, trade_date) DO NOTHING
        """, ('005930', date.date(), price-500, price+500, price-1000, price, 10000000+i*100000))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ 일별 데이터 30일치 생성 완료")

if __name__ == "__main__":
    save_daily_data()
