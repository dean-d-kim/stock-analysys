#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
코스닥 상위 20개 종목 데이터 수집
"""

import psycopg2
import os
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime, timedelta

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_kosdaq_top20():
    """코스닥 시가총액 상위 20개 종목 조회"""
    print("📊 코스닥 시가총액 상위 종목 조회 중...")

    # 최근 거래일 찾기 (최대 30일까지 확인)
    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            df = stock.get_market_cap_by_ticker(date, market='KOSDAQ')
            if not df.empty:
                # 시가총액 기준 상위 20개
                top20 = df.nlargest(20, '시가총액')
                # 실제 거래가 있는 날인지 확인 (시가총액 합계가 0이 아닌지)
                if top20['시가총액'].sum() > 0:
                    print(f"✅ {date} 기준 코스닥 상위 20개 종목 조회 완료")
                    return top20, date
                else:
                    print(f"  ⚠️  {date}: 거래 데이터 없음 (휴장일)")
        except Exception as e:
            # 너무 많은 에러 메시지 출력 방지
            if i < 5:
                print(f"  ⚠️  {date} 데이터 조회 실패: {e}")
            continue

    return None, None

def insert_stock(stock_code, stock_name, market_cap, market_type='KOSDAQ'):
    """종목 정보 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_cap, market_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_code)
            DO UPDATE SET
                market_cap = EXCLUDED.market_cap,
                market_type = EXCLUDED.market_type,
                stock_name = EXCLUDED.stock_name
        """, (stock_code, stock_name, market_cap, market_type))

        conn.commit()
    finally:
        cur.close()
        conn.close()

def collect_historical_data(stock_code, start_date, end_date):
    """종목별 과거 데이터 수집"""
    try:
        df = stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)

        if df.empty:
            return 0

        conn = get_db_connection()
        cur = conn.cursor()

        count = 0
        for date, row in df.iterrows():
            trade_date = date.strftime('%Y-%m-%d')

            cur.execute("""
                INSERT INTO daily_prices
                (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code, trade_date) DO NOTHING
            """, (
                stock_code,
                trade_date,
                int(row['시가']),
                int(row['고가']),
                int(row['저가']),
                int(row['종가']),
                int(row['거래량'])
            ))
            count += 1

        conn.commit()
        cur.close()
        conn.close()

        return count

    except Exception as e:
        print(f"    ❌ 데이터 수집 실패: {e}")
        return 0

def main():
    print("="*80)
    print("🚀 코스닥 상위 20개 종목 데이터 수집")
    print("="*80)
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. 코스닥 상위 20개 종목 조회
    top20_df, ref_date = get_kosdaq_top20()

    if top20_df is None:
        print("❌ 코스닥 데이터 조회 실패")
        return

    print(f"\n📋 수집 대상: {len(top20_df)}개 종목")
    print("-"*80)

    # 2. 각 종목 정보 저장 및 과거 데이터 수집
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')  # 6개월
    end_date = datetime.now().strftime('%Y%m%d')

    for idx, stock_code in enumerate(top20_df.index, 1):
        row = top20_df.loc[stock_code]
        stock_name = stock.get_market_ticker_name(stock_code)
        market_cap = int(row['시가총액'])

        print(f"[{idx}/20] {stock_name} ({stock_code})")
        print(f"  시가총액: {market_cap:,}원 ({market_cap/1_000_000_000_000:.1f}조)")

        # 종목 정보 저장
        insert_stock(stock_code, stock_name, market_cap, 'KOSDAQ')
        print(f"  ✅ 종목 정보 저장 완료")

        # 과거 데이터 수집
        print(f"  📈 과거 6개월 데이터 수집 중...")
        count = collect_historical_data(stock_code, start_date, end_date)
        print(f"  ✅ {count}건 데이터 수집 완료\n")

    print("="*80)
    print("✅ 코스닥 데이터 수집 완료")
    print("="*80)

if __name__ == "__main__":
    main()
