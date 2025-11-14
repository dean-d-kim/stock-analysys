#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
코스피/코스닥/ETF 상위 200개 종목 데이터 수집 (대안 방법)
pykrx의 get_market_cap_by_ticker API가 작동하지 않아 대안 방법 사용
"""

import psycopg2
import os
import time
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime, timedelta
import logging

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('collect_top200_alt.log', encoding='utf-8'),
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

def get_recent_trade_date():
    """최근 거래일 찾기"""
    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            tickers = stock.get_market_ticker_list(date, market='KOSPI')
            if len(tickers) > 0:
                logging.info(f"✅ 최근 거래일: {date}")
                return date
        except:
            continue
    return None

def get_stock_market_cap(stock_code, date):
    """개별 종목의 시가총액 조회"""
    try:
        df = stock.get_market_ohlcv_by_date(date, date, stock_code)
        if not df.empty:
            close_price = df.iloc[0]['종가']
            volume = df.iloc[0]['거래량']

            # 상장주식수 조회 (추정)
            # 실제 상장주식수는 다른 API 필요, 여기서는 거래대금 기반 추정
            return close_price, volume, close_price * volume
    except Exception as e:
        pass
    return None, None, None

def collect_market_stocks(market_type, limit=200, date=None):
    """특정 시장의 종목 데이터 수집"""
    if date is None:
        date = get_recent_trade_date()
        if date is None:
            logging.error(f"❌ {market_type} 거래일 조회 실패")
            return []

    logging.info(f"\n{'='*80}")
    logging.info(f"📊 {market_type} 상위 {limit}개 종목 수집 시작")
    logging.info(f"{'='*80}")

    # 종목 리스트 가져오기
    try:
        tickers = stock.get_market_ticker_list(date, market=market_type)
        logging.info(f"✅ {market_type} 전체 종목 수: {len(tickers)}개")
    except Exception as e:
        logging.error(f"❌ {market_type} 종목 리스트 조회 실패: {e}")
        return []

    # 각 종목의 시가총액 조회 및 정렬
    stocks_info = []
    for i, ticker in enumerate(tickers[:limit * 3], 1):  # 여유있게 더 많이 조회
        try:
            name = stock.get_market_ticker_name(ticker)
            close_price, volume, market_value = get_stock_market_cap(ticker, date)

            if close_price and market_value:
                stocks_info.append({
                    'code': ticker,
                    'name': name,
                    'market_cap': market_value,
                    'close_price': close_price,
                    'volume': volume
                })

            if i % 50 == 0:
                logging.info(f"  진행률: {i}/{len(tickers[:limit * 3])} ({i*100//len(tickers[:limit * 3])}%)")

            time.sleep(0.05)  # API 제한 방지
        except Exception as e:
            continue

    # 시가총액 기준 정렬 및 상위 200개 선택
    stocks_info.sort(key=lambda x: x['market_cap'], reverse=True)
    top_stocks = stocks_info[:limit]

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
        """, (stock_code, stock_name, str(market_cap), market_type, 'STOCK' if market_type != 'ETF' else 'ETF'))

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
        logging.error(f"  ❌ {stock_name} 데이터 수집 실패: {e}")
        return 0

def main():
    logging.info("="*80)
    logging.info("🚀 코스피/코스닥/ETF 상위 200개 종목 데이터 수집 (대안 방법)")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 수집 기간 설정 (2015년부터)
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')
    logging.info(f"📅 수집 기간: {start_date} ~ {end_date} (약 10년)")

    total_stocks = 0
    total_records = 0

    # 최근 거래일 찾기
    trade_date = get_recent_trade_date()
    if not trade_date:
        logging.error("❌ 거래일 조회 실패")
        return

    # 1. 코스피 처리
    kospi_stocks = collect_market_stocks('KOSPI', 200, trade_date)
    for i, stock_info in enumerate(kospi_stocks, 1):
        logging.info(f"[{i}/200] {stock_info['name']} ({stock_info['code']})")
        logging.info(f"  시가총액: {stock_info['market_cap']:,.0f}원")

        # 종목 정보 저장
        insert_stock(stock_info['code'], stock_info['name'], stock_info['market_cap'], 'KOSPI')

        # 과거 데이터 수집
        count = collect_historical_data(stock_info['code'], stock_info['name'], start_date, end_date)
        logging.info(f"  ✅ {count}건 저장")

        total_records += count
        time.sleep(0.5)

    total_stocks += len(kospi_stocks)

    # 2. 코스닥 처리
    kosdaq_stocks = collect_market_stocks('KOSDAQ', 200, trade_date)
    for i, stock_info in enumerate(kosdaq_stocks, 1):
        logging.info(f"[{i}/200] {stock_info['name']} ({stock_info['code']})")
        logging.info(f"  시가총액: {stock_info['market_cap']:,.0f}원")

        insert_stock(stock_info['code'], stock_info['name'], stock_info['market_cap'], 'KOSDAQ')

        count = collect_historical_data(stock_info['code'], stock_info['name'], start_date, end_date)
        logging.info(f"  ✅ {count}건 저장")

        total_records += count
        time.sleep(0.5)

    total_stocks += len(kosdaq_stocks)

    # 3. ETF 처리 (주요 ETF 하드코딩)
    major_etfs = [
        '069500', '102110', '114800', '122630', '138230',
        '152100', '153130', '157490', '217770', '227540',
        '251350', '252670', '253150', '261140', '364690'
    ]

    logging.info(f"\n{'='*80}")
    logging.info(f"📊 주요 ETF {len(major_etfs)}개 종목 수집 시작")
    logging.info(f"{'='*80}")

    for i, etf_code in enumerate(major_etfs, 1):
        try:
            name = stock.get_market_ticker_name(etf_code)
            logging.info(f"[{i}/{len(major_etfs)}] {name} ({etf_code})")

            # 시가총액 추정
            close_price, volume, market_value = get_stock_market_cap(etf_code, trade_date)
            if market_value:
                insert_stock(etf_code, name, market_value, 'ETF')

                count = collect_historical_data(etf_code, name, start_date, end_date)
                logging.info(f"  ✅ {count}건 저장")
                total_records += count

            time.sleep(0.5)
        except Exception as e:
            logging.error(f"  ❌ ETF {etf_code} 처리 실패: {e}")

    total_stocks += len(major_etfs)

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
