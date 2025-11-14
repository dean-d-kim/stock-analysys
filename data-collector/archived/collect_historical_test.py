from pykrx import stock
from db_handler import get_db_connection
from datetime import datetime
import time

def collect_stock_history(stock_code):
    """pykrx로 과거 데이터 수집"""
    try:
        df = stock.get_market_ohlcv('20200101', datetime.now().strftime('%Y%m%d'), stock_code)
        
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
    
    # 주식 상위 10개 (fetch_all_stocks에서 이미 시총순으로 수집됨)
    cur.execute("""
        SELECT stock_code, stock_name 
        FROM stocks 
        WHERE asset_type='STOCK' AND market_type='KOSPI'
        LIMIT 10
    """)
    stocks = cur.fetchall()
    
    # ETF 상위 10개
    cur.execute("""
        SELECT stock_code, stock_name 
        FROM stocks 
        WHERE asset_type='ETF' AND market_type='KOSPI'
        LIMIT 10
    """)
    etfs = cur.fetchall()
    
    cur.close()
    conn.close()
    
    all_items = stocks + etfs
    print(f"📊 테스트: KOSPI 주식 10개 + ETF 10개\n")
    
    for idx, (code, name) in enumerate(all_items, 1):
        print(f"[{idx}/{len(all_items)}] {name} ({code})")
        collect_stock_history(code)
        time.sleep(0.3)
    
    print("\n✅ 테스트 완료!")