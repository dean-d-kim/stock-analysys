#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공공데이터포털 API 데이터 제공 시작 연도 찾기
1995년부터 2025년까지 각 연도의 1월 첫 영업일 데이터 확인
"""

import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

API_BASE_URL = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'
API_KEY = os.getenv('DATA_GO_KR_API_KEY')

def check_data_for_date(date_str):
    """특정 날짜의 데이터 존재 여부 확인"""
    params = {
        'serviceKey': API_KEY,
        'numOfRows': 10,
        'pageNo': 1,
        'resultType': 'json',
        'basDt': date_str
    }

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if 'response' in data and 'body' in data['response']:
            body = data['response']['body']
            total_count = body.get('totalCount', 0)

            if total_count > 0:
                items = body.get('items', {}).get('item', [])
                if items:
                    return True, total_count

        return False, 0
    except Exception as e:
        print(f"  오류: {e}")
        return False, 0

def find_first_business_day(year):
    """해당 연도의 첫 영업일 찾기 (1월 1~15일 중에서)"""
    for day in range(1, 16):
        test_date = datetime(year, 1, day)

        # 주말 스킵
        if test_date.weekday() >= 5:
            continue

        date_str = test_date.strftime('%Y%m%d')
        has_data, count = check_data_for_date(date_str)

        if has_data:
            return date_str, count

    return None, 0

def main():
    print("="*80)
    print("📅 공공데이터포털 API 데이터 제공 시작 연도 확인")
    print("="*80)
    print(f"\n검색 기간: 1995년 ~ 2025년")
    print(f"방법: 각 연도 1월 첫 영업일 데이터 확인\n")
    print("-"*80)

    results = []

    for year in range(1995, 2026):
        print(f"\n{year}년 확인 중...", end=" ")

        date_str, count = find_first_business_day(year)

        if date_str:
            print(f"✅ 데이터 있음 - {date_str} ({count:,}건)")
            results.append((year, date_str, count, True))
        else:
            print(f"❌ 데이터 없음")
            results.append((year, None, 0, False))

    print("\n" + "="*80)
    print("📊 결과 요약")
    print("="*80)

    # 데이터가 있는 첫 해 찾기
    first_year_with_data = None
    for year, date_str, count, has_data in results:
        if has_data:
            first_year_with_data = year
            break

    if first_year_with_data:
        print(f"\n✅ 데이터 제공 시작 연도: {first_year_with_data}년")

        print(f"\n📋 데이터 제공 연도 목록:")
        for year, date_str, count, has_data in results:
            if has_data:
                print(f"  {year}년: {date_str} ({count:,}건)")
    else:
        print("\n❌ 데이터를 찾을 수 없습니다.")

    print("\n" + "="*80)

    # 권장 수집 기간
    if first_year_with_data:
        print(f"\n💡 권장 수집 기간: {first_year_with_data}년 1월 1일 ~ 현재")
        print(f"   - 예상 기간: 약 {2025 - first_year_with_data + 1}년치 데이터")
        print(f"   - 예상 영업일: 약 {(2025 - first_year_with_data + 1) * 250}일")

if __name__ == '__main__':
    main()
