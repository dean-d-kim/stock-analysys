#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_cap 컬럼 타입을 BIGINT로 변경
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

conn = get_db_connection()
cur = conn.cursor()

try:
    print("🔧 market_cap 컬럼 타입 변경 중...")

    # 문자열을 BIGINT로 변환
    cur.execute("""
        ALTER TABLE stocks
        ALTER COLUMN market_cap TYPE BIGINT USING market_cap::BIGINT
    """)

    conn.commit()
    print("✅ market_cap 컬럼 타입이 BIGINT로 변경되었습니다")

    # 확인
    print("\n📊 상위 5개 종목 확인:")
    cur.execute("""
        SELECT stock_code, stock_name, market_cap
        FROM stocks
        ORDER BY market_cap DESC NULLS LAST
        LIMIT 5
    """)

    for idx, row in enumerate(cur.fetchall(), 1):
        code, name, market_cap = row
        market_cap_trillion = market_cap / 1000000000000 if market_cap else 0
        print(f"{idx}. {name:20s} ({code}) - {market_cap_trillion:6.1f}조")

except Exception as e:
    conn.rollback()
    print(f"❌ 오류: {e}")
finally:
    cur.close()
    conn.close()
