#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
코스피200/코스닥150/ETF200 종목 데이터 수집 (배치 처리 버전)
DB 연결 문제 해결 - 배치 단위로 커밋
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
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!'),
    'connect_timeout': 30,
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

def get_db_connection():
    """DB 연결 - 타임아웃 및 keepalive 설정"""
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

def insert_stock_batch(stocks_data):
    """종목 정보 배치 삽입"""
    if not stocks_data:
        return

    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            for stock_code, stock_name, market_type in stocks_data:
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
            logging.info(f"  ✅ {len(stocks_data)}개 종목 정보 저장")
            return

        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"  ❌ 배치 저장 실패 (시도 {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 지수 백오프
        finally:
            if conn:
                cur.close()
                conn.close()

    logging.error(f"  ❌ {len(stocks_data)}개 종목 정보 저장 최종 실패")

def insert_daily_price_batch(prices_data):
    """일별 시세 배치 삽입"""
    if not prices_data:
        return 0

    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            inserted = 0
            for stock_code, trade_date, open_p, high_p, low_p, close_p, volume in prices_data:
                cur.execute("""
                    INSERT INTO daily_prices (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stock_code, trade_date) DO NOTHING
                """, (stock_code, trade_date, int(open_p), int(high_p), int(low_p), int(close_p), int(volume)))
                inserted += cur.rowcount

            conn.commit()
            return inserted

        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"  ❌ 가격 배치 저장 실패 (시도 {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        finally:
            if conn:
                cur.close()
                conn.close()

    return 0

def collect_historical_data_batch(stock_code, stock_name, start_date, end_date, batch_size=100):
    """과거 데이터 수집 (배치 처리)"""
    try:
        df = stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)

        if df.empty:
            return 0

        total_count = 0
        prices_batch = []

        for date, row in df.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            prices_batch.append((
                stock_code, date_str,
                row['시가'], row['고가'], row['저가'], row['종가'], row['거래량']
            ))

            # 배치 크기에 도달하면 DB 저장
            if len(prices_batch) >= batch_size:
                count = insert_daily_price_batch(prices_batch)
                total_count += count
                prices_batch = []

        # 남은 데이터 저장
        if prices_batch:
            count = insert_daily_price_batch(prices_batch)
            total_count += count

        return total_count

    except Exception as e:
        logging.error(f"  ❌ 데이터 수집 실패: {e}")
        return 0

def get_stock_name(stock_code):
    """종목명 조회"""
    try:
        name = stock.get_market_ticker_name(stock_code)
        return name if name else f"종목_{stock_code}"
    except:
        return f"종목_{stock_code}"

def get_kosdaq150(trade_date):
    """코스닥150 지수 구성 종목 가져오기"""
    kosdaq150 = stock.get_index_portfolio_deposit_file('2203', trade_date)
    logging.info(f"  코스닥150 지수: {len(kosdaq150)}개")
    return list(kosdaq150)

def get_etf_top200(trade_date):
    """ETF 상위 200개 가져오기 (거래대금 기준)"""
    try:
        konex_tickers = stock.get_market_ticker_list(trade_date, market='KONEX')
    except:
        konex_tickers = []

    etf_stocks = []
    for ticker in konex_tickers[:300]:
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

    etf_stocks.sort(key=lambda x: x['trading_value'], reverse=True)
    return [s['code'] for s in etf_stocks[:200]]

def main():
    logging.info("="*80)
    logging.info("🚀 코스피200/코스닥150/ETF200 데이터 수집 (배치 처리)")
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

    # 1. 코스피200
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 코스피200 지수 구성 종목 수집")
    logging.info(f"{'='*80}")

    kospi200 = stock.get_index_portfolio_deposit_file('1028', trade_date)
    logging.info(f"✅ 코스피200: {len(kospi200)}개 종목")

    # 종목 정보 배치 저장 (10개씩)
    stock_batch_size = 10
    for i in range(0, len(kospi200), stock_batch_size):
        batch_codes = kospi200[i:i+stock_batch_size]
        stocks_data = []

        for code in batch_codes:
            name = get_stock_name(code)
            stocks_data.append((code, name, 'KOSPI'))

        insert_stock_batch(stocks_data)
        time.sleep(0.5)

    # 가격 데이터 수집
    for i, code in enumerate(kospi200, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[코스피 {i}/200] {name} ({code})")

            count = collect_historical_data_batch(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.3)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(kospi200)

    # 2. 코스닥150
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 코스닥150 지수 구성 종목 수집")
    logging.info(f"{'='*80}")

    kosdaq150 = get_kosdaq150(trade_date)
    logging.info(f"✅ 코스닥150: {len(kosdaq150)}개 종목")

    # 종목 정보 배치 저장
    for i in range(0, len(kosdaq150), stock_batch_size):
        batch_codes = kosdaq150[i:i+stock_batch_size]
        stocks_data = []

        for code in batch_codes:
            name = get_stock_name(code)
            stocks_data.append((code, name, 'KOSDAQ'))

        insert_stock_batch(stocks_data)
        time.sleep(0.5)

    # 가격 데이터 수집
    for i, code in enumerate(kosdaq150, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[코스닥 {i}/150] {name} ({code})")

            count = collect_historical_data_batch(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.3)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(kosdaq150)

    # 3. ETF 200개
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 ETF 상위 200개 수집 (거래대금 기준)")
    logging.info(f"{'='*80}")

    etf200 = get_etf_top200(trade_date)
    logging.info(f"✅ ETF: {len(etf200)}개 종목")

    # 종목 정보 배치 저장
    for i in range(0, len(etf200), stock_batch_size):
        batch_codes = etf200[i:i+stock_batch_size]
        stocks_data = []

        for code in batch_codes:
            name = get_stock_name(code)
            stocks_data.append((code, name, 'ETF'))

        insert_stock_batch(stocks_data)
        time.sleep(0.5)

    # 가격 데이터 수집
    for i, code in enumerate(etf200, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[ETF {i}/{len(etf200)}] {name} ({code})")

            count = collect_historical_data_batch(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.3)
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
