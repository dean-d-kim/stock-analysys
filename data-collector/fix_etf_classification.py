#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF 분류 수정 스크립트
잘못 분류된 종목들을 올바른 market_type으로 재분류
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# DB 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

# ETF 키워드 (일반 증권사명 KB, NH, MIRAE, KIWOOM 제외)
ETF_KEYWORDS = ['KODEX', 'TIGER', 'ARIRANG', 'KBSTAR', 'KOSEF', 'TREX', 'SOL ', 'ACE ',
               'TIMEFOLIO', 'RISE', 'PLUS', 'HANARO', 'SMART', 'KINDEX', 'SYNTH',
               'TRUE', 'MULTI', 'FOCUS', 'ITF', 'ALPHA', 'KTOP', 'QV',
               '1Q', 'HK ', '마이티', '에셋플러스']

def fix_etf_classification():
    """ETF 분류 수정"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("ETF 분류 수정 시작...")
        print("=" * 80)

        # 1. 현재 상태 확인
        print("\n[현재 상태]")
        cur.execute('SELECT market_type, COUNT(*) FROM stocks GROUP BY market_type ORDER BY market_type')
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}개")

        # 2. ETF로 잘못 분류된 종목 찾기 (ETF 키워드가 없는데 ETF로 분류된 것)
        print("\n[ETF로 잘못 분류된 종목 찾기]")
        cur.execute('SELECT stock_code, stock_name, market_type FROM stocks WHERE market_type = %s', ('ETF',))
        wrong_etfs = []
        correct_etfs = []

        for row in cur.fetchall():
            stock_code, stock_name, market_type = row
            is_etf = any(keyword in stock_name for keyword in ETF_KEYWORDS)

            if not is_etf:
                wrong_etfs.append((stock_code, stock_name))
            else:
                correct_etfs.append((stock_code, stock_name))

        print(f"  올바른 ETF: {len(correct_etfs)}개")
        print(f"  잘못 분류된 종목: {len(wrong_etfs)}개")

        if wrong_etfs:
            print("\n  잘못 분류된 종목 샘플 (최대 10개):")
            for stock_code, stock_name in wrong_etfs[:10]:
                print(f"    {stock_code} - {stock_name}")

        # 3. 일반 종목 중 ETF 키워드가 있는 것 찾기
        print("\n[일반 종목 중 ETF로 분류해야 할 종목 찾기]")
        cur.execute('''
            SELECT stock_code, stock_name, market_type
            FROM stocks
            WHERE market_type != 'ETF'
        ''')

        should_be_etf = []
        for row in cur.fetchall():
            stock_code, stock_name, market_type = row
            is_etf = any(keyword in stock_name for keyword in ETF_KEYWORDS)

            if is_etf:
                should_be_etf.append((stock_code, stock_name, market_type))

        print(f"  ETF로 재분류해야 할 종목: {len(should_be_etf)}개")

        if should_be_etf:
            print("\n  샘플 (최대 10개):")
            for stock_code, stock_name, old_market_type in should_be_etf[:10]:
                print(f"    {stock_code} - {stock_name} ({old_market_type} -> ETF)")

        # 4. 수정 적용
        print("\n[수정 적용]")

        # 4-1. 잘못된 ETF를 KONEX로 변경 (대부분 KONEX 출신)
        if wrong_etfs:
            for stock_code, stock_name in wrong_etfs:
                cur.execute('''
                    UPDATE stocks
                    SET market_type = 'KONEX'
                    WHERE stock_code = %s
                ''', (stock_code,))
            print(f"  ✅ {len(wrong_etfs)}개 종목을 ETF -> KONEX로 변경")

        # 4-2. ETF로 분류해야 할 종목을 ETF로 변경
        if should_be_etf:
            for stock_code, stock_name, old_market_type in should_be_etf:
                cur.execute('''
                    UPDATE stocks
                    SET market_type = 'ETF'
                    WHERE stock_code = %s
                ''', (stock_code,))
            print(f"  ✅ {len(should_be_etf)}개 종목을 ETF로 변경")

        conn.commit()

        # 5. 최종 상태 확인
        print("\n[최종 상태]")
        cur.execute('SELECT market_type, COUNT(*) FROM stocks GROUP BY market_type ORDER BY market_type')
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}개")

        print("\n" + "=" * 80)
        print("✅ ETF 분류 수정 완료!")

        cur.close()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ 오류 발생: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    fix_etf_classification()
