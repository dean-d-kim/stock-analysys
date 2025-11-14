#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 종목 순서 확인
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

print("📊 stocks 테이블 상위 20개 (시가총액 내림차순)")
print("="*80)

cur.execute("""
    SELECT stock_code, stock_name, market_cap, display_rank
    FROM stocks
    ORDER BY market_cap DESC NULLS LAST
    LIMIT 20
""")

for idx, row in enumerate(cur.fetchall(), 1):
    code, name, market_cap, rank = row
    market_cap_trillion = market_cap / 1000000000000 if market_cap else 0
    print(f"{idx:2d}. {name:20s} ({code}) - {market_cap_trillion:6.1f}조 (순위: {rank})")

cur.close()
conn.close()
