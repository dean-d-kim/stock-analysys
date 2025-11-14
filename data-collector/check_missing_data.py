#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
최근 30일 데이터 누락 확인 스크립트
"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

# DB 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

def check_missing_data():
    """최근 30일 데이터 누락 확인"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("="*80)
        print("최근 30일 데이터 누락 확인")
        print("="*80)

        # 1. 종목 수 확인
        print("\n[1. 종목 수]")
        cur.execute('''
            SELECT market_type, COUNT(*)
            FROM stocks
            WHERE market_type != 'KONEX'
            GROUP BY market_type
            ORDER BY market_type
        ''')

        market_counts = {}
        for row in cur.fetchall():
            market_counts[row[0]] = row[1]
            print(f"  {row[0]:10s}: {row[1]:5d}개")

        # 2. 최근 30일 날짜별 데이터 현황
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)

        print(f"\n[2. 최근 30일 데이터 현황 ({start_date} ~ {end_date})]")

        cur.execute('''
            SELECT
                trade_date,
                COUNT(DISTINCT dp.stock_code) as total_count,
                COUNT(DISTINCT CASE WHEN s.market_type = 'KOSPI' THEN dp.stock_code END) as kospi_count,
                COUNT(DISTINCT CASE WHEN s.market_type = 'KOSDAQ' THEN dp.stock_code END) as kosdaq_count,
                COUNT(DISTINCT CASE WHEN s.market_type = 'ETF' THEN dp.stock_code END) as etf_count
            FROM daily_prices dp
            JOIN stocks s ON dp.stock_code = s.stock_code
            WHERE trade_date >= %s AND trade_date <= %s
              AND s.market_type != 'KONEX'
            GROUP BY trade_date
            ORDER BY trade_date DESC
        ''', (start_date, end_date))

        date_data = cur.fetchall()

        if date_data:
            print(f"\n  {'날짜':<12} {'전체':>8} {'KOSPI':>8} {'KOSDAQ':>8} {'ETF':>8} {'상태'}")
            print("  " + "-"*60)
            for row in date_data:
                status = ""
                if row[2] < market_counts.get('KOSPI', 0) * 0.9:
                    status += "⚠️KOSPI "
                if row[3] < market_counts.get('KOSDAQ', 0) * 0.9:
                    status += "⚠️KOSDAQ "
                if row[4] < market_counts.get('ETF', 0) * 0.9:
                    status += "⚠️ETF "
                if not status:
                    status = "✅"
                print(f"  {row[0]!s:<12} {row[1]:>8} {row[2]:>8} {row[3]:>8} {row[4]:>8} {status}")
        else:
            print("  최근 30일 데이터 없음")

        # 3. 누락된 날짜 확인
        print(f"\n[3. 누락된 거래일 확인]")

        # 모든 평일 생성 (주말 제외)
        all_dates = []
        current = start_date
        while current <= end_date:
            # 주말(토:5, 일:6) 제외
            if current.weekday() < 5:
                all_dates.append(current)
            current += timedelta(days=1)

        # DB에 있는 날짜
        cur.execute('''
            SELECT DISTINCT trade_date
            FROM daily_prices
            WHERE trade_date >= %s AND trade_date <= %s
            ORDER BY trade_date
        ''', (start_date, end_date))

        existing_dates = {row[0] for row in cur.fetchall()}

        missing_dates = [d for d in all_dates if d not in existing_dates]

        if missing_dates:
            print(f"  ⚠️ 누락된 평일: {len(missing_dates)}일")
            for missing_date in missing_dates:
                weekday_name = ['월', '화', '수', '목', '금', '토', '일'][missing_date.weekday()]
                print(f"    {missing_date} ({weekday_name})")
        else:
            print("  ✅ 누락된 평일 없음")

        # 4. 최신 데이터 날짜
        print(f"\n[4. 최신 데이터 날짜]")
        cur.execute('''
            SELECT
                s.market_type,
                MAX(dp.trade_date) as latest_date,
                COUNT(DISTINCT dp.stock_code) as stock_count
            FROM daily_prices dp
            JOIN stocks s ON dp.stock_code = s.stock_code
            WHERE s.market_type != 'KONEX'
            GROUP BY s.market_type
            ORDER BY s.market_type
        ''')

        for row in cur.fetchall():
            print(f"  {row[0]:10s}: {row[1]} ({row[2]}개 종목)")

        # 5. 가격 데이터 없는 종목 확인
        print(f"\n[5. 가격 데이터 없는 종목]")
        cur.execute('''
            SELECT
                s.market_type,
                COUNT(*) as no_price_count
            FROM stocks s
            LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
            WHERE dp.stock_code IS NULL
              AND s.market_type != 'KONEX'
            GROUP BY s.market_type
            ORDER BY s.market_type
        ''')

        no_price_data = cur.fetchall()
        if no_price_data:
            for row in no_price_data:
                print(f"  ⚠️ {row[0]:10s}: {row[1]:5d}개")

                # 샘플 출력
                cur.execute('''
                    SELECT stock_code, stock_name
                    FROM stocks
                    WHERE market_type = %s
                      AND stock_code NOT IN (SELECT DISTINCT stock_code FROM daily_prices)
                    LIMIT 5
                ''', (row[0],))

                samples = cur.fetchall()
                for sample in samples:
                    print(f"      {sample[0]} - {sample[1]}")
        else:
            print("  ✅ 모든 종목에 가격 데이터 있음")

        print("\n" + "="*80)

        # 누락 날짜 반환
        return missing_dates

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    missing_dates = check_missing_data()

    if missing_dates:
        print(f"\n📝 수집이 필요한 날짜: {len(missing_dates)}일")
        print("다음 명령어로 데이터를 수집하세요:")
        print("  python3 collect_data_go_kr.py")
        print("  python3 collect_etf_go_kr.py")
