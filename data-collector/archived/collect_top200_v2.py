#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
코스피/코스닥 상위 200개 종목 수집 (v2 - 개선된 방법)
전체 종목 리스트를 가져온 후 시가총액 계산하여 상위 200개 선정
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

# 로깅 설정 (unbuffered)
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
    """최근 거래일 찾기 - 실제 거래 데이터로 검증"""
    for i in range(30):  # 30일까지 확장
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            # 삼성전자(005930)로 실제 거래일 확인
            df = stock.get_market_ohlcv_by_date(date, date, '005930')
            if not df.empty:  # 실제 거래 데이터가 있는 경우만 거래일로 인정
                logging.info(f"✅ 최근 거래일: {date}")
                return date
        except Exception as e:
            if i < 5:
                logging.info(f"  {date}: 거래 데이터 없음")
            continue
    return None

def get_market_cap_for_stock(stock_code, date):
    """개별 종목의 시가총액 계산 (종가 × 거래량으로 추정)"""
    try:
        # 최근 5일 데이터 조회 (휴장일 대비)
        end_date = date
        start_date = (datetime.strptime(date, '%Y%m%d') - timedelta(days=5)).strftime('%Y%m%d')

        df = stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)
        if df.empty:
            return None

        # 가장 최근 데이터 사용
        last_row = df.iloc[-1]
        close_price = last_row['종가']
        volume = last_row['거래량']

        # 시가총액 추정: 종가 * 거래량 (상대적 크기 비교용)
        estimated_market_cap = close_price * volume

        return {
            'close_price': close_price,
            'volume': volume,
            'market_cap': estimated_market_cap
        }
    except Exception as e:
        return None

def collect_top_stocks(market_type, target_count=200, trade_date=None):
    """시장별 상위 종목 수집"""
    if trade_date is None:
        trade_date = get_recent_trade_date()

    logging.info(f"\n{'='*80}")
    logging.info(f"📊 {market_type} 상위 {target_count}개 종목 수집")
    logging.info(f"{'='*80}")

    # 1. 전체 종목 리스트 가져오기
    try:
        all_tickers = stock.get_market_ticker_list(trade_date, market=market_type)
        logging.info(f"✅ {market_type} 전체 종목: {len(all_tickers)}개")
    except Exception as e:
        logging.error(f"❌ {market_type} 종목 리스트 조회 실패: {e}")
        return []

    # 2. 각 종목의 시가총액 계산
    stocks_with_cap = []
    batch_size = 100

    for i in range(0, min(len(all_tickers), target_count * 2), batch_size):
        batch = all_tickers[i:i+batch_size]
        logging.info(f"  진행: {i}/{min(len(all_tickers), target_count * 2)} ({i*100//min(len(all_tickers), target_count * 2)}%)")

        for ticker in batch:
            try:
                # 종목명 조회 (실패 시 스킵)
                try:
                    name = stock.get_market_ticker_name(ticker)
                    if not name:
                        continue
                except:
                    continue

                # 시가총액 정보 조회
                cap_info = get_market_cap_for_stock(ticker, trade_date)

                if cap_info and cap_info['market_cap'] > 0:
                    stocks_with_cap.append({
                        'code': ticker,
                        'name': name,
                        'market_cap': cap_info['market_cap'],
                        'close_price': cap_info['close_price'],
                        'volume': cap_info['volume']
                    })

                time.sleep(0.1)  # API 제한 방지 (더 안전하게 증가)
            except Exception as e:
                continue

    # 3. 시가총액 기준 정렬 및 상위 N개 선택
    stocks_with_cap.sort(key=lambda x: x['market_cap'], reverse=True)
    top_stocks = stocks_with_cap[:target_count]

    logging.info(f"✅ {market_type} 상위 {len(top_stocks)}개 종목 선정 완료")
    return top_stocks

def insert_stock(stock_code, stock_name, market_cap, market_type):
    """종목 정보 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_cap, market_type, asset_type)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (stock_code)
            DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                market_cap = EXCLUDED.market_cap,
                market_type = EXCLUDED.market_type
        """, (stock_code, stock_name, str(market_cap), market_type, 'STOCK'))

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

def main():
    logging.info("="*80)
    logging.info("🚀 코스피/코스닥 상위 200개 종목 수집 (v2)")
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

    # 코스피 상위 200개
    kospi_stocks = collect_top_stocks('KOSPI', 200, trade_date)
    for i, stock_info in enumerate(kospi_stocks, 1):
        logging.info(f"[코스피 {i}/200] {stock_info['name']} ({stock_info['code']})")

        # 종목 정보 저장
        insert_stock(stock_info['code'], stock_info['name'], stock_info['market_cap'], 'KOSPI')

        # 과거 데이터 수집
        count = collect_historical_data(stock_info['code'], stock_info['name'], start_date, end_date)
        logging.info(f"  ✅ {count}건 저장")

        total_records += count
        time.sleep(0.5)

    total_stocks += len(kospi_stocks)

    # 코스닥 상위 200개
    kosdaq_stocks = collect_top_stocks('KOSDAQ', 200, trade_date)
    for i, stock_info in enumerate(kosdaq_stocks, 1):
        logging.info(f"[코스닥 {i}/200] {stock_info['name']} ({stock_info['code']})")

        insert_stock(stock_info['code'], stock_info['name'], stock_info['market_cap'], 'KOSDAQ')

        count = collect_historical_data(stock_info['code'], stock_info['name'], start_date, end_date)
        logging.info(f"  ✅ {count}건 저장")

        total_records += count
        time.sleep(0.5)

    total_stocks += len(kosdaq_stocks)

    # ETF 상위 200개
    etf_stocks = collect_top_stocks('ETF', 200, trade_date)
    for i, stock_info in enumerate(etf_stocks, 1):
        logging.info(f"[ETF {i}/200] {stock_info['name']} ({stock_info['code']})")

        insert_stock(stock_info['code'], stock_info['name'], stock_info['market_cap'], 'ETF')

        count = collect_historical_data(stock_info['code'], stock_info['name'], start_date, end_date)
        logging.info(f"  ✅ {count}건 저장")

        total_records += count
        time.sleep(0.5)

    total_stocks += len(etf_stocks)

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 전체 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 종목 수: {total_stocks}개 (KOSPI: {len(kospi_stocks)}, KOSDAQ: {len(kosdaq_stocks)}, ETF: {len(etf_stocks)})")
    logging.info(f"총 레코드 수: {total_records}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
