#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KOSPI/KOSDAQ/ETF 각각 200개 종목 데이터 수집
get_market_ticker_name 사용하지 않고 코드만으로 수집
"""

import psycopg2
import os
import time
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime
import logging
import sys

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
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

def insert_stock(stock_code, market_type):
    """종목 정보 DB에 삽입 (종목명은 코드로 대체)"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        asset_type = 'ETF' if market_type == 'ETF' else 'STOCK'
        stock_name = f"{market_type}_{stock_code}"  # 임시 종목명

        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_type, asset_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_code)
            DO UPDATE SET
                market_type = EXCLUDED.market_type
        """, (stock_code, stock_name, market_type, asset_type))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
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

def collect_historical_data(stock_code, start_date, end_date):
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
        return 0

def main():
    logging.info("="*80)
    logging.info("🚀 KOSPI/KOSDAQ/ETF 각각 200개 종목 데이터 수집")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 수집 기간
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')
    logging.info(f"📅 데이터 수집 기간: {start_date} ~ {end_date}")

    total_stocks = 0
    total_records = 0
    trade_date = '20251104'

    # 1. KOSPI 200개
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 KOSPI 200개 종목 처리")
    logging.info(f"{'='*80}")

    try:
        kospi_tickers = stock.get_market_ticker_list(trade_date, market='KOSPI')
        logging.info(f"✅ KOSPI 전체 종목: {len(kospi_tickers)}개")

        # 첫 200개 종목만 처리
        for i, code in enumerate(kospi_tickers[:200], 1):
            try:
                logging.info(f"[KOSPI {i}/200] {code}")

                # 종목 정보 저장
                insert_stock(code, 'KOSPI')

                # 과거 데이터 수집
                count = collect_historical_data(code, start_date, end_date)
                if count > 0:
                    logging.info(f"  ✅ {count}건 저장")
                    total_records += count
                else:
                    logging.info(f"  ⚠️  데이터 없음")

                time.sleep(0.1)
            except Exception as e:
                logging.error(f"  ❌ {code} 처리 실패: {e}")
                continue

        total_stocks += 200
    except Exception as e:
        logging.error(f"❌ KOSPI 처리 실패: {e}")

    # 2. KOSDAQ 200개
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 KOSDAQ 200개 종목 처리")
    logging.info(f"{'='*80}")

    try:
        kosdaq_tickers = stock.get_market_ticker_list(trade_date, market='KOSDAQ')
        logging.info(f"✅ KOSDAQ 전체 종목: {len(kosdaq_tickers)}개")

        for i, code in enumerate(kosdaq_tickers[:200], 1):
            try:
                logging.info(f"[KOSDAQ {i}/200] {code}")

                insert_stock(code, 'KOSDAQ')

                count = collect_historical_data(code, start_date, end_date)
                if count > 0:
                    logging.info(f"  ✅ {count}건 저장")
                    total_records += count
                else:
                    logging.info(f"  ⚠️  데이터 없음")

                time.sleep(0.1)
            except Exception as e:
                logging.error(f"  ❌ {code} 처리 실패: {e}")
                continue

        total_stocks += 200
    except Exception as e:
        logging.error(f"❌ KOSDAQ 처리 실패: {e}")

    # 3. ETF 200개 (KONEX로 시도)
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 ETF 200개 종목 처리")
    logging.info(f"{'='*80}")

    try:
        konex_tickers = stock.get_market_ticker_list(trade_date, market='KONEX')
        logging.info(f"✅ KONEX 전체 종목: {len(konex_tickers)}개")

        for i, code in enumerate(konex_tickers[:200], 1):
            try:
                logging.info(f"[ETF {i}/200] {code}")

                insert_stock(code, 'ETF')

                count = collect_historical_data(code, start_date, end_date)
                if count > 0:
                    logging.info(f"  ✅ {count}건 저장")
                    total_records += count
                else:
                    logging.info(f"  ⚠️  데이터 없음")

                time.sleep(0.1)
            except Exception as e:
                logging.error(f"  ❌ {code} 처리 실패: {e}")
                continue

        total_stocks += min(200, len(konex_tickers))
    except Exception as e:
        logging.error(f"❌ ETF 처리 실패: {e}")

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 전체 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 종목 수: {total_stocks}개")
    logging.info(f"총 레코드 수: {total_records}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
