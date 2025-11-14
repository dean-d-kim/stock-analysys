#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 히스토리 수집 테스트 - 1주일치만 수집
"""

import sys
import os

# 현재 디렉토리를 sys.path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from collect_full_historical import collect_full_historical

def main():
    print("="*80)
    print("📋 전체 히스토리 수집 테스트 (1주일)")
    print("="*80)

    # 최근 1주일 데이터로 테스트
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=7)

    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    print(f"\n테스트 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print("\n시작합니다...\n")

    try:
        total_records, success_days = collect_full_historical(start_str, end_str)

        print("\n" + "="*80)
        print("✅ 테스트 완료")
        print("="*80)
        print(f"총 레코드: {total_records:,}건")
        print(f"성공한 날: {success_days:,}일")
        print("="*80)
        print("\n다음 단계: check_db_status.py를 실행하여 데이터를 확인하세요.")

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
