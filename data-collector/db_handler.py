import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('../.env')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def save_stock(stock_code, stock_name, market_type):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stocks (stock_code, stock_name, market_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (stock_code) DO NOTHING
    """, (stock_code, stock_name, market_type))
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {stock_name} 저장 완료")

if __name__ == "__main__":
    save_stock("005930", "삼성전자", "KOSPI")
