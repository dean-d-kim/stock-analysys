#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 종목 분류 확인 및 수정 스크립트
KB증권, NHN 등 잘못 분류된 종목을 올바른 시장으로 재분류
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

# ETF 키워드 (정확한 ETF 브랜드명만)
ETF_KEYWORDS = ['KODEX', 'TIGER', 'ARIRANG', 'KBSTAR', 'KOSEF', 'TREX', 'SOL ', 'ACE ',
               'TIMEFOLIO', 'RISE', 'PLUS', 'HANARO', 'SMART', 'KINDEX', 'SYNTH',
               'TRUE', 'MULTI', 'FOCUS', 'ITF', 'ALPHA', 'KTOP', 'QV',
               '1Q', 'HK ', '마이티', '에셋플러스']

def verify_and_fix_classification():
    """종목 분류 확인 및 수정"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("종목 분류 확인 및 수정 시작...")
        print("=" * 80)

        # 1. 현재 상태 확인
        print("\n[현재 상태]")
        cur.execute('SELECT market_type, COUNT(*) FROM stocks GROUP BY market_type ORDER BY market_type')
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}개")

        # 2. KONEX로 잘못 분류된 종목 찾기
        print("\n[KONEX 종목 중 ETF 키워드가 있는 종목]")
        cur.execute('SELECT stock_code, stock_name, market_type FROM stocks WHERE market_type = %s', ('KONEX',))
        konex_stocks = cur.fetchall()

        wrong_konex_etfs = []
        for stock_code, stock_name, market_type in konex_stocks:
            is_etf = any(keyword in stock_name for keyword in ETF_KEYWORDS)
            if is_etf:
                wrong_konex_etfs.append((stock_code, stock_name))

        print(f"  ETF로 재분류해야 할 KONEX 종목: {len(wrong_konex_etfs)}개")
        if wrong_konex_etfs:
            print("\n  샘플 (최대 10개):")
            for stock_code, stock_name in wrong_konex_etfs[:10]:
                print(f"    {stock_code} - {stock_name}")

        # 3. ETF로 잘못 분류된 종목 찾기 (ETF 키워드가 없는 종목)
        print("\n[ETF로 잘못 분류된 종목]")
        cur.execute('SELECT stock_code, stock_name FROM stocks WHERE market_type = %s', ('ETF',))
        etf_stocks = cur.fetchall()

        wrong_etfs = []
        for stock_code, stock_name in etf_stocks:
            is_etf = any(keyword in stock_name for keyword in ETF_KEYWORDS)
            if not is_etf:
                wrong_etfs.append((stock_code, stock_name))

        print(f"  일반 종목으로 재분류해야 할 ETF: {len(wrong_etfs)}개")
        if wrong_etfs:
            print("\n  샘플 (최대 20개):")
            for stock_code, stock_name in wrong_etfs[:20]:
                print(f"    {stock_code} - {stock_name}")

        # 4. 공공데이터 API에서 원래 시장 정보 확인
        # API로 최신 데이터를 가져와서 원래 market_type을 확인해야 함
        # 일단 수동으로 주요 종목만 수정

        # KB증권, NHN 등 주요 종목 수동 수정
        major_fixes = [
            ('008770', 'KOSPI'),  # KB증권
            ('181710', 'KOSPI'),  # NHN
            ('105560', 'KOSPI'),  # KB금융
            ('005940', 'KOSPI'),  # NH투자증권
        ]

        print("\n[주요 종목 수동 수정]")
        for stock_code, correct_market in major_fixes:
            cur.execute('SELECT stock_name, market_type FROM stocks WHERE stock_code = %s', (stock_code,))
            result = cur.fetchone()
            if result:
                stock_name, current_market = result
                if current_market != correct_market:
                    print(f"  {stock_code} - {stock_name}: {current_market} -> {correct_market}")
                    cur.execute('UPDATE stocks SET market_type = %s WHERE stock_code = %s',
                               (correct_market, stock_code))

        # 5. 수정 적용
        print("\n[자동 수정 적용]")

        # KONEX의 ETF를 ETF로 변경
        if wrong_konex_etfs:
            for stock_code, stock_name in wrong_konex_etfs:
                cur.execute('UPDATE stocks SET market_type = %s WHERE stock_code = %s',
                           ('ETF', stock_code))
            print(f"  ✅ {len(wrong_konex_etfs)}개 종목을 KONEX -> ETF로 변경")

        conn.commit()

        # 6. 최종 상태 확인
        print("\n[최종 상태]")
        cur.execute('SELECT market_type, COUNT(*) FROM stocks GROUP BY market_type ORDER BY market_type')
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}개")

        # 7. ETF 탭 확인 (ETF 키워드 없는 종목 리스트)
        print("\n[ETF 탭에서 제외해야 할 종목 (ETF 키워드 없음)]")
        cur.execute('SELECT stock_code, stock_name, market_type FROM stocks WHERE market_type = %s', ('ETF',))
        etf_check = cur.fetchall()

        non_etf_in_etf = []
        for stock_code, stock_name, market_type in etf_check:
            is_etf = any(keyword in stock_name for keyword in ETF_KEYWORDS)
            if not is_etf:
                non_etf_in_etf.append((stock_code, stock_name))

        if non_etf_in_etf:
            print(f"  총 {len(non_etf_in_etf)}개")
            print("\n  전체 목록:")
            for stock_code, stock_name in non_etf_in_etf:
                print(f"    {stock_code} - {stock_name}")
        else:
            print("  ✅ ETF 탭에 ETF 키워드 없는 종목이 없습니다!")

        print("\n" + "=" * 80)
        print("✅ 종목 분류 확인 및 수정 완료!")

        cur.close()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ 오류 발생: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    verify_and_fix_classification()
