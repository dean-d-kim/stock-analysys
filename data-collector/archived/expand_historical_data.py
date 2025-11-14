#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB에 저장된 종목들의 과거 데이터를 2015년부터 수집
"""

import psycopg2
import os
import time
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime
import logging

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('expand_historical.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_all_stocks():
    """DB에 저장된 모든 종목 조회"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT stock_code, stock_name, market_type
            FROM stocks
            ORDER BY market_cap DESC NULLS LAST
        """)
        stocks = cur.fetchall()
        return stocks
    finally:
        cur.close()
        conn.close()

def insert_daily_price(stock_code, trade_date, open_p, high_p, low_p, close_p, volume):
    """일별 시세 데이터 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO daily_prices (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, trade_date) DO NOTHING
        """, (stock_code, trade_date, int(open_p), int(high_p), int(low_p), int(close_p), int(volume)))

        conn.commit()
        return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def collect_historical_data(stock_code, stock_name, start_date, end_date):
    """과거 데이터 수집"""
    try:
        df = stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)

        if df.empty:
            return 0

        count = 0
        for date, row in df.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            inserted = insert_daily_price(
                stock_code, date_str,
                row['시가'], row['고가'], row['저가'], row['종가'], row['거래량']
            )
            count += inserted

        return count
    except Exception as e:
        logging.error(f"  ❌ 데이터 수집 실패: {e}")
        return 0

def main():
    logging.info("="*80)
    logging.info("🚀 DB 저장 종목의 과거 데이터 확장 (2015년~현재)")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 수집 기간 설정
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')
    logging.info(f"📅 수집 기간: {start_date} ~ {end_date}")

    # DB에서 종목 목록 조회
    stocks = get_all_stocks()
    logging.info(f"📋 총 {len(stocks)}개 종목")
    logging.info("="*80)

    total_records = 0

    for i, (stock_code, stock_name, market_type) in enumerate(stocks, 1):
        logging.info(f"[{i}/{len(stocks)}] {stock_name} ({stock_code}) - {market_type}")

        try:
            # 과거 데이터 수집
            count = collect_historical_data(stock_code, stock_name, start_date, end_date)
            total_records += count
            logging.info(f"  ✅ {count}건 저장 (신규 데이터)")

            # API 제한 방지
            time.sleep(0.5)

        except Exception as e:
            logging.error(f"  ❌ {stock_name} 처리 실패: {e}")
            continue

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 데이터 확장 완료")
    logging.info(f"{'='*80}")
    logging.info(f"처리 종목 수: {len(stocks)}개")
    logging.info(f"신규 레코드 수: {total_records}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
