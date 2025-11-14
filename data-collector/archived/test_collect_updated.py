#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업데이트된 데이터 수집 스크립트 테스트 - 1일치만 수집
"""

import os
import sys
from datetime import datetime, timedelta

# 현재 디렉토리를 sys.path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# collect_data_go_kr 모듈 임포트
from collect_data_go_kr import collect_date_range as collect_stocks
from collect_etf_go_kr import collect_date_range as collect_etfs

def main():
    print("="*80)
    print("📋 업데이트된 스크립트 테스트")
    print("="*80)

    # 어제 날짜로 테스트 (최근 완전한 데이터)
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y%m%d')

    print(f"\n테스트 날짜: {yesterday.strftime('%Y-%m-%d')}")
    print("\n1️⃣ 주식 데이터 수집 테스트...")
    print("-"*80)

    try:
        stock_records = collect_stocks(date_str, date_str)
        print(f"✅ 주식 데이터 수집 완료: {stock_records}건")
    except Exception as e:
        print(f"❌ 주식 데이터 수집 실패: {e}")

    print("\n2️⃣ ETF 데이터 수집 테스트...")
    print("-"*80)

    try:
        etf_records = collect_etfs(date_str, date_str)
        print(f"✅ ETF 데이터 수집 완료: {etf_records}건")
    except Exception as e:
        print(f"❌ ETF 데이터 수집 실패: {e}")

    print("\n"+"="*80)
    print("✅ 테스트 완료")
    print("="*80)
    print("\n다음 단계: check_db_status.py를 실행하여 새 필드가 저장되었는지 확인하세요.")

if __name__ == '__main__':
    main()
