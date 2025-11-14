#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PER(주가수익비율)와 PBR(주가순자산비율) 컬럼 추가 및 데이터 입력
"""

import psycopg2
import os
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime, timedelta

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

def add_per_pbr_columns():
    """stocks 테이블에 PER, PBR 컬럼 추가"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        print("📊 stocks 테이블에 PER, PBR 컬럼 추가 중...")

        # PER 컬럼 추가 확인
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='per'
        """)

        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE stocks
                ADD COLUMN per DECIMAL(10,2)
            """)
            print("  ✅ PER 컬럼 추가 완료")
        else:
            print("  ℹ️  PER 컬럼이 이미 존재합니다")

        # PBR 컬럼 추가 확인
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='pbr'
        """)

        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE stocks
                ADD COLUMN pbr DECIMAL(10,2)
            """)
            print("  ✅ PBR 컬럼 추가 완료")
        else:
            print("  ℹ️  PBR 컬럼이 이미 존재합니다")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"❌ 컬럼 추가 실패: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def get_latest_trading_date():
    """최근 거래일 찾기"""
    for i in range(10):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            # 간단한 조회로 거래일 확인
            df = stock.get_market_fundamental_by_ticker(date, market="KOSPI")
            if not df.empty:
                return date
        except:
            continue
    return None

def update_per_pbr_data():
    """PER, PBR 데이터 업데이트"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        print("\n💰 PER, PBR 데이터 수집 중...")

        # 최근 거래일 찾기
        trade_date = get_latest_trading_date()
        if not trade_date:
            print("❌ 최근 거래일을 찾을 수 없습니다")
            return

        print(f"  거래일: {trade_date}")

        # 모든 종목 조회
        cur.execute("SELECT stock_code, stock_name FROM stocks")
        stocks = cur.fetchall()

        success_count = 0
        fail_count = 0

        for stock_code, stock_name in stocks:
            try:
                # pykrx로 PER, PBR 데이터 조회
                df = stock.get_market_fundamental_by_ticker(trade_date, market="KOSPI")

                if stock_code in df.index:
                    per_value = df.loc[stock_code, 'PER']
                    pbr_value = df.loc[stock_code, 'PBR']

                    # NULL 값 처리
                    per_value = None if per_value == 0 or per_value > 1000 else per_value
                    pbr_value = None if pbr_value == 0 or pbr_value > 100 else pbr_value

                    cur.execute("""
                        UPDATE stocks
                        SET per = %s, pbr = %s
                        WHERE stock_code = %s
                    """, (per_value, pbr_value, stock_code))

                    per_str = f"{per_value:.2f}" if per_value else "N/A"
                    pbr_str = f"{pbr_value:.2f}" if pbr_value else "N/A"
                    print(f"  ✓ {stock_name:20s} ({stock_code}) - PER: {per_str:>8s}, PBR: {pbr_str:>8s}")
                    success_count += 1
                else:
                    print(f"  ⚠️  {stock_name:20s} ({stock_code}) - 데이터 없음")
                    fail_count += 1

            except Exception as e:
                print(f"  ❌ {stock_name:20s} ({stock_code}) - 오류: {e}")
                fail_count += 1
                continue

        conn.commit()
        print(f"\n✅ PER/PBR 데이터 업데이트 완료 (성공: {success_count}, 실패: {fail_count})")

    except Exception as e:
        conn.rollback()
        print(f"❌ 데이터 업데이트 실패: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def show_summary():
    """PER, PBR 데이터 요약"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        print("\n" + "="*80)
        print("📊 PER/PBR 데이터 요약 (상위 10개 종목)")
        print("="*80)

        cur.execute("""
            SELECT stock_code, stock_name, market_cap, per, pbr
            FROM stocks
            ORDER BY market_cap DESC NULLS LAST
            LIMIT 10
        """)

        print(f"{'순위':<4} {'종목명':<20} {'종목코드':<10} {'시가총액':<12} {'PER':<10} {'PBR':<10}")
        print("-" * 80)

        for idx, row in enumerate(cur.fetchall(), 1):
            code, name, market_cap, per, pbr = row
            market_cap_str = f"{market_cap/1000000000000:.1f}조" if market_cap else "N/A"
            per_str = f"{per:.2f}" if per else "N/A"
            pbr_str = f"{pbr:.2f}" if pbr else "N/A"

            print(f"{idx:<4} {name:<20} {code:<10} {market_cap_str:<12} {per_str:<10} {pbr_str:<10}")

        print("=" * 80)

    except Exception as e:
        print(f"❌ 요약 실패: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    print("="*80)
    print("🚀 PER/PBR 데이터 추가 시작")
    print("="*80)

    try:
        # 1. PER, PBR 컬럼 추가
        add_per_pbr_columns()

        # 2. PER, PBR 데이터 수집 및 업데이트
        update_per_pbr_data()

        # 3. 결과 요약
        show_summary()

        print("\n✅ 모든 작업 완료!")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
