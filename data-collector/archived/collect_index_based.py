#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
코스피200/코스닥150/ETF200 종목 데이터 수집
지수 구성 종목 기반 (시가총액 기준)
"""

import psycopg2
import os
import time
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime, timedelta
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

def get_recent_trade_date():
    """최근 거래일 찾기"""
    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            df = stock.get_market_ohlcv_by_date(date, date, '005930')
            if not df.empty:
                logging.info(f"✅ 최근 거래일: {date}")
                return date
        except:
            continue
    return None

def insert_stock(stock_code, stock_name, market_type):
    """종목 정보 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        asset_type = 'ETF' if market_type == 'ETF' else 'STOCK'
        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_type, asset_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_code)
            DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
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

def get_stock_name(stock_code):
    """종목명 조회 - 임시 종목명 사용"""
    # get_market_ticker_name() 사용 안 함 (API 불안정)
    return f"종목_{stock_code}"

def get_kosdaq150(trade_date):
    """코스닥150 지수 구성 종목 가져오기"""
    kosdaq150 = stock.get_index_portfolio_deposit_file('2203', trade_date)
    logging.info(f"  코스닥150 지수: {len(kosdaq150)}개")
    return list(kosdaq150)

def get_etf_top200(trade_date):
    """ETF 상위 200개 가져오기 (거래대금 기준)"""
    # KONEX에서 ETF 목록 가져오기
    try:
        konex_tickers = stock.get_market_ticker_list(trade_date, market='KONEX')
    except:
        konex_tickers = []

    # 거래대금 계산
    etf_stocks = []
    for ticker in konex_tickers[:300]:  # 상위 300개 확인
        try:
            df = stock.get_market_ohlcv_by_date(trade_date, trade_date, ticker)
            if not df.empty:
                close = int(df.iloc[0]['종가'])
                volume = int(df.iloc[0]['거래량'])
                trading_value = close * volume
                etf_stocks.append({
                    'code': ticker,
                    'trading_value': trading_value
                })
            time.sleep(0.05)
        except:
            pass

    # 거래대금 기준 정렬 후 상위 200개
    etf_stocks.sort(key=lambda x: x['trading_value'], reverse=True)
    return [s['code'] for s in etf_stocks[:200]]

def main():
    logging.info("="*80)
    logging.info("🚀 코스피200/코스닥150/ETF200 데이터 수집 (지수 기반)")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 수집 기간
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')
    logging.info(f"📅 데이터 수집 기간: {start_date} ~ {end_date}")

    trade_date = get_recent_trade_date()
    if not trade_date:
        logging.error("❌ 거래일 조회 실패")
        return

    total_stocks = 0
    total_records = 0

    # 1. 코스피200 (지수 구성 종목)
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 코스피200 지수 구성 종목 수집")
    logging.info(f"{'='*80}")

    kospi200 = stock.get_index_portfolio_deposit_file('1028', trade_date)
    logging.info(f"✅ 코스피200: {len(kospi200)}개 종목")

    for i, code in enumerate(kospi200, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[코스피 {i}/200] {name} ({code})")

            insert_stock(code, name, 'KOSPI')

            count = collect_historical_data(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.2)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(kospi200)

    # 2. 코스닥150 (지수 구성 종목)
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 코스닥150 지수 구성 종목 수집")
    logging.info(f"{'='*80}")

    kosdaq150 = get_kosdaq150(trade_date)
    logging.info(f"✅ 코스닥150: {len(kosdaq150)}개 종목")

    for i, code in enumerate(kosdaq150, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[코스닥 {i}/150] {name} ({code})")

            insert_stock(code, name, 'KOSDAQ')

            count = collect_historical_data(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.2)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(kosdaq150)

    # 3. ETF 200개 (거래대금 기준)
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 ETF 상위 200개 수집 (거래대금 기준)")
    logging.info(f"{'='*80}")

    etf200 = get_etf_top200(trade_date)
    logging.info(f"✅ ETF: {len(etf200)}개 종목")

    for i, code in enumerate(etf200, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[ETF {i}/{len(etf200)}] {name} ({code})")

            insert_stock(code, name, 'ETF')

            count = collect_historical_data(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.2)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(etf200)

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 전체 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 종목 수: {total_stocks}개 (KOSPI: {len(kospi200)}, KOSDAQ: {len(kosdaq150)}, ETF: {len(etf200)})")
    logging.info(f"총 레코드 수: {total_records}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
