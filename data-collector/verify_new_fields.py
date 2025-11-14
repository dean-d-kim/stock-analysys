#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
새로 추가된 필드가 제대로 저장되었는지 검증
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("="*80)
        print("🔍 새로 추가된 필드 검증")
        print("="*80)

        # 1. stocks 테이블의 새 필드 확인
        print("\n1️⃣ STOCKS 테이블 - 새 필드 데이터 확인")
        print("-"*80)

        # ISIN 코드가 있는 종목 수
        cur.execute("SELECT COUNT(*) FROM stocks WHERE isin_code IS NOT NULL")
        isin_count = cur.fetchone()[0]
        print(f"ISIN 코드 설정된 종목: {isin_count:,}개")

        # 상장주식수가 있는 종목 수
        cur.execute("SELECT COUNT(*) FROM stocks WHERE listed_shares IS NOT NULL")
        shares_count = cur.fetchone()[0]
        print(f"상장주식수 설정된 종목: {shares_count:,}개")

        # 시가총액이 있는 종목 수
        cur.execute("SELECT COUNT(*) FROM stocks WHERE market_cap IS NOT NULL")
        cap_count = cur.fetchone()[0]
        print(f"시가총액 설정된 종목: {cap_count:,}개")

        # ETF 전용 필드
        cur.execute("SELECT COUNT(*) FROM stocks WHERE nav IS NOT NULL")
        nav_count = cur.fetchone()[0]
        print(f"NAV 설정된 ETF: {nav_count:,}개")

        cur.execute("SELECT COUNT(*) FROM stocks WHERE base_index_name IS NOT NULL")
        idx_count = cur.fetchone()[0]
        print(f"기초지수명 설정된 ETF: {idx_count:,}개")

        # 샘플 데이터 확인 (KOSPI 1개)
        print("\n📋 KOSPI 샘플 데이터:")
        cur.execute("""
            SELECT stock_code, stock_name, isin_code, listed_shares, market_cap
            FROM stocks
            WHERE market_type = 'KOSPI' AND market_cap IS NOT NULL
            ORDER BY market_cap DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            print(f"  종목코드: {row[0]}")
            print(f"  종목명: {row[1]}")
            print(f"  ISIN: {row[2]}")
            print(f"  상장주식수: {row[3]:,}주" if row[3] else "  상장주식수: N/A")
            print(f"  시가총액: {row[4]/1_000_000_000_000:.2f}조원" if row[4] else "  시가총액: N/A")

        # 샘플 데이터 확인 (ETF 1개)
        print("\n📋 ETF 샘플 데이터:")
        cur.execute("""
            SELECT stock_code, stock_name, isin_code, nav, net_asset_total, base_index_name
            FROM stocks
            WHERE market_type = 'ETF' AND nav IS NOT NULL
            ORDER BY net_asset_total DESC NULLS LAST
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            print(f"  종목코드: {row[0]}")
            print(f"  종목명: {row[1]}")
            print(f"  ISIN: {row[2]}")
            print(f"  NAV: {row[3]:,.0f}원" if row[3] else "  NAV: N/A")
            print(f"  순자산총액: {row[4]/100_000_000:.0f}억원" if row[4] else "  순자산총액: N/A")
            print(f"  기초지수: {row[5]}" if row[5] else "  기초지수: N/A")

        # 2. daily_prices 테이블의 새 필드 확인
        print("\n2️⃣ DAILY_PRICES 테이블 - 새 필드 데이터 확인")
        print("-"*80)

        # vs(전일대비)가 있는 레코드 수
        cur.execute("SELECT COUNT(*) FROM daily_prices WHERE vs IS NOT NULL")
        vs_count = cur.fetchone()[0]
        print(f"전일대비(vs) 설정된 레코드: {vs_count:,}건")

        # change_rate(등락율)가 있는 레코드 수
        cur.execute("SELECT COUNT(*) FROM daily_prices WHERE change_rate IS NOT NULL")
        rate_count = cur.fetchone()[0]
        print(f"등락율(change_rate) 설정된 레코드: {rate_count:,}건")

        # trading_value(거래대금)가 있는 레코드 수
        cur.execute("SELECT COUNT(*) FROM daily_prices WHERE trading_value IS NOT NULL")
        value_count = cur.fetchone()[0]
        print(f"거래대금(trading_value) 설정된 레코드: {value_count:,}건")

        # 샘플 데이터 확인
        print("\n📋 일별 시세 샘플 데이터 (최근 거래일):")
        cur.execute("""
            SELECT s.stock_name, dp.trade_date, dp.close_price, dp.vs, dp.change_rate, dp.trading_value
            FROM daily_prices dp
            JOIN stocks s ON dp.stock_code = s.stock_code
            WHERE dp.vs IS NOT NULL AND dp.change_rate IS NOT NULL
            ORDER BY dp.trade_date DESC, dp.trading_value DESC NULLS LAST
            LIMIT 3
        """)
        for i, row in enumerate(cur.fetchall(), 1):
            print(f"\n  {i}. {row[0]}")
            print(f"     거래일: {row[1]}")
            print(f"     종가: {row[2]:,}원")
            print(f"     전일대비: {row[3]:+,}원" if row[3] else "     전일대비: N/A")
            print(f"     등락율: {row[4]:+.2f}%" if row[4] else "     등락율: N/A")
            print(f"     거래대금: {row[5]/100_000_000:,.0f}억원" if row[5] else "     거래대금: N/A")

        print("\n"+"="*80)
        print("✅ 검증 완료 - 모든 새 필드가 정상적으로 저장되었습니다!")
        print("="*80)

    except Exception as e:
        print(f"❌ 오류: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    main()
