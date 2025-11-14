#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB 스키마 업데이트 - API 제공 모든 데이터 저장
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
        print("🔧 DB 스키마 업데이트")
        print("="*80)

        # 1. stocks 테이블에 컬럼 추가
        print("\n1️⃣ stocks 테이블 컬럼 추가...")

        # ISIN 코드
        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS isin_code VARCHAR(20)
        """)
        print("  ✅ isin_code 추가")

        # 상장주식수/상장좌수
        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS listed_shares BIGINT
        """)
        print("  ✅ listed_shares 추가")

        # ETF 전용 필드들
        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS nav NUMERIC(15, 2)
        """)
        print("  ✅ nav (순자산가치) 추가")

        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS net_asset_total BIGINT
        """)
        print("  ✅ net_asset_total (순자산총액) 추가")

        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS base_index_name VARCHAR(200)
        """)
        print("  ✅ base_index_name (기초지수명) 추가")

        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS base_index_close NUMERIC(15, 2)
        """)
        print("  ✅ base_index_close (기초지수종가) 추가")

        # 2. daily_prices 테이블에 컬럼 추가
        print("\n2️⃣ daily_prices 테이블 컬럼 추가...")

        # 전일대비
        cur.execute("""
            ALTER TABLE daily_prices
            ADD COLUMN IF NOT EXISTS vs INTEGER
        """)
        print("  ✅ vs (전일대비) 추가")

        # 등락율
        cur.execute("""
            ALTER TABLE daily_prices
            ADD COLUMN IF NOT EXISTS change_rate NUMERIC(10, 2)
        """)
        print("  ✅ change_rate (등락율) 추가")

        # trading_value가 이미 있는지 확인하고 없으면 추가
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'daily_prices'
            AND column_name = 'trading_value'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE daily_prices
                ADD COLUMN trading_value BIGINT
            """)
            print("  ✅ trading_value (거래대금) 추가")
        else:
            print("  ℹ️  trading_value (거래대금) 이미 존재")

        # 3. 인덱스 추가
        print("\n3️⃣ 인덱스 생성...")

        # ISIN 코드 인덱스
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stocks_isin_code
            ON stocks(isin_code)
        """)
        print("  ✅ stocks.isin_code 인덱스")

        # 상장주식수 인덱스
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stocks_listed_shares
            ON stocks(listed_shares)
        """)
        print("  ✅ stocks.listed_shares 인덱스")

        conn.commit()

        # 4. 업데이트된 스키마 확인
        print("\n4️⃣ 업데이트된 스키마 확인:")

        print("\n📋 stocks 테이블:")
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'stocks'
            ORDER BY ordinal_position
        """)
        for row in cur.fetchall():
            max_len = f'({row[2]})' if row[2] else ''
            print(f"  {row[0]:25s} {row[1]}{max_len}")

        print("\n📋 daily_prices 테이블:")
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'daily_prices'
            ORDER BY ordinal_position
        """)
        for row in cur.fetchall():
            max_len = f'({row[2]})' if row[2] else ''
            print(f"  {row[0]:25s} {row[1]}{max_len}")

        print(f"\n{'='*80}")
        print("✅ 스키마 업데이트 완료")
        print(f"{'='*80}")

    except Exception as e:
        print(f"❌ 오류: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    main()
