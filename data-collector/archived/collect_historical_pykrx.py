from pykrx import stock
from db_handler import get_db_connection
from datetime import datetime
import time

def collect_stock_history(stock_code, start_date='20200101'):
    """pykrx로 과거 데이터 수집"""
    end_date = datetime.now().strftime('%Y%m%d')
    
    try:
        # OHLCV 데이터 가져오기
        df = stock.get_market_ohlcv(start_date, end_date, stock_code)
        
        if df.empty:
            print(f"⚠️  {stock_code}: 데이터 없음")
            return
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        for date, row in df.iterrows():
            cur.execute("""
                INSERT INTO daily_prices 
                (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code, trade_date) DO NOTHING
            """, (
                stock_code,
                date.strftime('%Y-%m-%d'),
                int(row['시가']),
                int(row['고가']),
                int(row['저가']),
                int(row['종가']),
                int(row['거래량'])
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ {stock_code}: {len(df)}일 저장")
        
    except Exception as e:
        print(f"❌ {stock_code}: {e}")

if __name__ == "__main__":
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 전체 주식만 (ETF 제외)
    cur.execute("SELECT stock_code, stock_name FROM stocks WHERE asset_type='STOCK'")
    stocks = cur.fetchall()
    cur.close()
    conn.close()
    
    print(f"📊 총 {len(stocks)}개 종목 수집 시작...\n")
    
    for idx, (code, name) in enumerate(stocks, 1):
        print(f"[{idx}/{len(stocks)}] {name} ({code})")
        collect_stock_history(code)
        time.sleep(0.3)  # 서버 부담 완화
    
    print("\n🎉 전체 수집 완료!")