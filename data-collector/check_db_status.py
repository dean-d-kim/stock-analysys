#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB 상태 확인 및 시가총액 계산
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("="*80)
        print("📊 DB 상태 확인")
        print("="*80)

        # 1. 시장별 종목 수
        cur.execute("""
            SELECT market_type, COUNT(*) as count
            FROM stocks
            GROUP BY market_type
            ORDER BY market_type
        """)
        print("\n1️⃣ 시장별 종목 수:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,}개")

        # 2. 최근 거래일 확인
        cur.execute("""
            SELECT MAX(trade_date) as last_date
            FROM daily_prices
        """)
        last_date = cur.fetchone()[0]
        print(f"\n2️⃣ 최근 거래일: {last_date}")

        # 3. 일별 시세 레코드 수
        cur.execute("SELECT COUNT(*) FROM daily_prices")
        price_count = cur.fetchone()[0]
        print(f"3️⃣ 일별 시세 레코드: {price_count:,}건")

        # 4. 시가총액이 null인 종목 수
        cur.execute("SELECT COUNT(*) FROM stocks WHERE market_cap IS NULL")
        null_cap = cur.fetchone()[0]
        print(f"4️⃣ 시가총액 미설정 종목: {null_cap:,}개")

        # 5. 시가총액 있는 종목 수 확인
        print(f"\n5️⃣ 시가총액 통계:")
        cur.execute("SELECT COUNT(*) FROM stocks WHERE market_cap IS NOT NULL AND market_cap > 0")
        cap_count = cur.fetchone()[0]
        print(f"  시가총액 설정된 종목: {cap_count:,}개")

        # 6. 업데이트 후 시가총액 상위 10개 확인
        print(f"\n6️⃣ KOSPI 시가총액 상위 10개:")
        cur.execute("""
            SELECT stock_code, stock_name, market_cap
            FROM stocks
            WHERE market_type = 'KOSPI'
            ORDER BY market_cap DESC NULLS LAST
            LIMIT 10
        """)
        for i, row in enumerate(cur.fetchall(), 1):
            cap_trillion = row[2] / 1_000_000_000_000 if row[2] else 0
            print(f"  {i}. {row[1]} ({row[0]}): {cap_trillion:.2f}조")

        print(f"\n7️⃣ KOSDAQ 시가총액 상위 10개:")
        cur.execute("""
            SELECT stock_code, stock_name, market_cap
            FROM stocks
            WHERE market_type = 'KOSDAQ'
            ORDER BY market_cap DESC NULLS LAST
            LIMIT 10
        """)
        for i, row in enumerate(cur.fetchall(), 1):
            cap_hundred_million = row[2] / 100_000_000 if row[2] else 0
            print(f"  {i}. {row[1]} ({row[0]}): {cap_hundred_million:.0f}억")

        print(f"\n8️⃣ ETF 거래대금 상위 10개:")
        cur.execute("""
            SELECT s.stock_code, s.stock_name,
                   dp.close_price, dp.volume,
                   (CAST(dp.close_price AS BIGINT) * dp.volume) as trading_value
            FROM stocks s
            JOIN LATERAL (
                SELECT close_price, volume
                FROM daily_prices
                WHERE stock_code = s.stock_code
                ORDER BY trade_date DESC
                LIMIT 1
            ) dp ON true
            WHERE s.market_type = 'ETF'
            ORDER BY trading_value DESC NULLS LAST
            LIMIT 10
        """)
        for i, row in enumerate(cur.fetchall(), 1):
            trading_billion = row[4] / 100_000_000 if row[4] else 0
            print(f"  {i}. {row[1]} ({row[0]}): {trading_billion:.0f}억원")

        print(f"\n{'='*80}")
        print("✅ DB 상태 확인 완료")
        print(f"{'='*80}")

    except Exception as e:
        print(f"❌ 오류: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    main()
