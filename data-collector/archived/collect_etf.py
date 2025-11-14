#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF 상위 20개 종목 데이터 수집 (거래대금 기준)
"""

import psycopg2
import os
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd

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

def get_etf_top20():
    """ETF 거래대금 상위 20개 종목 조회 (대체 방법)"""
    print("📊 ETF 거래대금 상위 종목 조회 중...")

    # 최근 거래일 찾기 (최대 30일까지 확인)
    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            # 전체 시장에서 ETF 코드 추출 (ETF는 주로 특정 패턴)
            # KOSPI와 KOSDAQ에서 모두 조회
            etf_data = []

            # 알려진 주요 ETF 코드 리스트 (거래량 상위)
            major_etfs = [
                '069500',  # KODEX 200
                '102110',  # TIGER 200
                '114800',  # KODEX 인버스
                '122630',  # KODEX 레버리지
                '251340',  # KODEX 코스닥150레버리지
                '229200',  # KODEX 코스닥150
                '233740',  # KODEX 코스닥150레버리지
                '278530',  # KODEX 200선물인버스2X
                '252670',  # KODEX 200선물인버스2X
                '371460',  # TIGER 차이나전기차SOLACTIVE
                '364690',  # KINDEX 미국S&P500
                '360750',  # TIGER 미국S&P500
                '143850',  # TIGER 200IT
                '148020',  # KBSTAR 200
                '232080',  # TIGER 200선물인버스2X
                '069660',  # KOSEF 200
                '091160',  # KODEX 반도체
                '091180',  # KODEX 자동차
                '091170',  # KODEX 은행
                '168490',  # HANARO Fn KOSPI200
            ]

            print(f"  📅 {date} 데이터 조회 중...")

            for etf_code in major_etfs:
                try:
                    # ETF OHLCV 데이터 조회
                    df = stock.get_etf_ohlcv_by_date(date, date, etf_code)

                    if not df.empty:
                        row = df.iloc[0]
                        etf_name = stock.get_etf_ticker_name(etf_code)

                        # 거래대금 = 종가 * 거래량
                        trading_value = int(row['종가'] * row['거래량'])

                        if trading_value > 0:
                            etf_data.append({
                                '종목코드': etf_code,
                                '종목명': etf_name,
                                '시가총액': trading_value,
                                '종가': int(row['종가']),
                                '거래량': int(row['거래량'])
                            })
                except Exception as e:
                    continue

            if len(etf_data) >= 20:
                df_result = pd.DataFrame(etf_data)
                top20 = df_result.nlargest(20, '시가총액')
                print(f"✅ {date} 기준 ETF 상위 20개 조회 완료 (총 {len(etf_data)}개 중)")
                return top20, date
            elif len(etf_data) > 0:
                print(f"  ⚠️  {date}: 데이터 {len(etf_data)}개만 조회됨 (20개 미만)")
                # 20개 미만이어도 데이터가 있으면 반환
                df_result = pd.DataFrame(etf_data)
                top_available = df_result.nlargest(min(20, len(etf_data)), '시가총액')
                print(f"✅ {date} 기준 ETF 상위 {len(top_available)}개 조회 완료")
                return top_available, date
            else:
                print(f"  ⚠️  {date}: 거래 데이터 없음")

        except Exception as e:
            if i < 5:
                print(f"  ⚠️  {date} 데이터 조회 실패: {e}")
            continue

    return None, None

def insert_stock(stock_code, stock_name, market_cap, market_type='ETF'):
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

def collect_historical_data(etf_code, start_date, end_date):
    """ETF별 과거 데이터 수집"""
    try:
        df = stock.get_etf_ohlcv_by_date(start_date, end_date, etf_code)

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
                etf_code,
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
    print("🚀 ETF 상위 20개 종목 데이터 수집")
    print("="*80)
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. ETF 상위 20개 종목 조회
    top20_df, ref_date = get_etf_top20()

    if top20_df is None:
        print("❌ ETF 데이터 조회 실패")
        return

    print(f"\n📋 수집 대상: {len(top20_df)}개 종목")
    print("-"*80)

    # 2. 각 종목 정보 저장 및 과거 데이터 수집
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')  # 6개월
    end_date = datetime.now().strftime('%Y%m%d')

    for idx, (_, row) in enumerate(top20_df.iterrows(), 1):
        etf_code = row['종목코드']
        etf_name = row['종목명']
        trading_value = row['시가총액']

        print(f"[{idx}/{len(top20_df)}] {etf_name} ({etf_code})")
        print(f"  거래대금: {trading_value:,}원 ({trading_value/100_000_000:.0f}억)")

        # 종목 정보 저장
        insert_stock(etf_code, etf_name, trading_value, 'ETF')
        print(f"  ✅ 종목 정보 저장 완료")

        # 과거 데이터 수집
        print(f"  📈 과거 6개월 데이터 수집 중...")
        count = collect_historical_data(etf_code, start_date, end_date)
        print(f"  ✅ {count}건 데이터 수집 완료\n")

    print("="*80)
    print("✅ ETF 데이터 수집 완료")
    print("="*80)

if __name__ == "__main__":
    main()
