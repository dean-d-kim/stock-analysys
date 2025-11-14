#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pykrx에서 실시간 시가총액 업데이트
"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pykrx import stock
import time

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

def get_market_cap_from_pykrx(stock_code):
    """pykrx에서 시가총액 조회"""
    # 최근 거래일 찾기 (최대 15일 전까지)
    for i in range(15):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')

        # KOSPI와 KOSDAQ 모두 시도
        for market in ['KOSPI', 'KOSDAQ']:
            try:
                df = stock.get_market_cap_by_ticker(date, market=market)
                if not df.empty and stock_code in df.index:
                    row = df.loc[stock_code]

                    # 시가총액 컬럼 확인
                    if isinstance(row, dict) or hasattr(row, 'index'):
                        if isinstance(row, dict):
                            market_cap = row.get('시가총액', 0)
                        else:
                            market_cap = row.get('시가총액', 0) if '시가총액' in row.index else 0

                        if market_cap > 0:
                            return int(market_cap)
            except Exception as e:
                # 에러는 무시하고 계속
                pass
    return None

def update_market_cap(stock_code, market_cap):
    """stocks 테이블의 시가총액 업데이트"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE stocks
            SET market_cap = %s
            WHERE stock_code = %s
        """, (market_cap, stock_code))

        conn.commit()
    finally:
        cur.close()
        conn.close()

def main():
    print("="*80)
    print("🚀 pykrx 기반 시가총액 업데이트")
    print("="*80)
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # DB에서 종목 목록 조회
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT stock_code, stock_name FROM stocks ORDER BY stock_code")
    stocks = cur.fetchall()
    cur.close()
    conn.close()

    print(f"📊 총 {len(stocks)}개 종목 처리 시작\n")

    success_count = 0
    fail_count = 0

    for idx, (stock_code, stock_name) in enumerate(stocks, 1):
        print(f"[{idx}/{len(stocks)}] {stock_name} ({stock_code})")

        # pykrx에서 시가총액 조회
        market_cap = get_market_cap_from_pykrx(stock_code)

        if market_cap:
            # 조 단위로 표시
            trillion = market_cap / 1_000_000_000_000
            print(f"  ✅ 시가총액: {market_cap:,}원 ({trillion:.1f}조)")

            # DB 업데이트
            update_market_cap(stock_code, market_cap)
            print(f"  ✅ DB 업데이트 완료\n")
            success_count += 1
        else:
            print(f"  ❌ 시가총액 조회 실패\n")
            fail_count += 1

        time.sleep(0.1)  # API 과부하 방지

    print("="*80)
    print(f"✅ 처리 완료")
    print(f"   성공: {success_count}개")
    print(f"   실패: {fail_count}개")
    print("="*80)

    # 결과 확인
    print_summary()

def print_summary():
    """업데이트 결과 요약"""
    conn = get_db_connection()
    cur = conn.cursor()

    print("\n" + "="*80)
    print("📊 시가총액 상위 10개 종목")
    print("="*80)

    cur.execute("""
        SELECT stock_code, stock_name, market_cap
        FROM stocks
        ORDER BY market_cap DESC NULLS LAST
        LIMIT 10
    """)

    print(f"{'순위':<4} {'종목명':<15} {'종목코드':<10} {'시가총액 (조)':<15}")
    print("-"*80)

    for idx, (code, name, market_cap) in enumerate(cur.fetchall(), 1):
        if market_cap:
            trillion = market_cap / 1_000_000_000_000
            print(f"{idx:<4} {name:<15} {code:<10} {trillion:>14.1f}조")
        else:
            print(f"{idx:<4} {name:<15} {code:<10} {'N/A':>15}")

    print("="*80)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
