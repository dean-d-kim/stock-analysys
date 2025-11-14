#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stocks 테이블에 market_type 컬럼 추가 및 기존 데이터 업데이트
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

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def main():
    print("="*80)
    print("🔧 DB 스키마 업데이트: market_type 컬럼 추가")
    print("="*80)

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # market_type 컬럼 추가
        print("\n1. market_type 컬럼 추가 중...")
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='market_type'
        """)

        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE stocks
                ADD COLUMN market_type VARCHAR(10) DEFAULT 'KOSPI'
            """)
            print("  ✅ market_type 컬럼 추가 완료")
        else:
            print("  ℹ️  market_type 컬럼이 이미 존재합니다")

        # 기존 코스피 데이터를 'KOSPI'로 업데이트
        print("\n2. 기존 데이터 market_type 업데이트 중...")
        cur.execute("""
            UPDATE stocks
            SET market_type = 'KOSPI'
            WHERE market_type IS NULL OR market_type = ''
        """)

        updated_count = cur.rowcount
        print(f"  ✅ {updated_count}개 종목이 KOSPI로 업데이트되었습니다")

        conn.commit()

        # 결과 확인
        print("\n3. 현재 DB 상태 확인:")
        cur.execute("""
            SELECT market_type, COUNT(*) as count
            FROM stocks
            GROUP BY market_type
            ORDER BY count DESC
        """)

        print("\n  시장별 종목 수:")
        for market_type, count in cur.fetchall():
            print(f"    {market_type}: {count}개")

        print("\n" + "="*80)
        print("✅ DB 스키마 업데이트 완료")
        print("="*80)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ 오류 발생: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
