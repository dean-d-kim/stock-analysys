#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공공데이터포털 API 테스트 - 인코딩/디코딩 키 모두 테스트
"""

import requests
from datetime import datetime, timedelta

# API 설정
API_BASE_URL = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'

# 두 가지 키
DECODING_KEY = 'XN8k4WOG6Hcfyvgh2FPeLeWfM97FH7XoMLNY81YJU8K9WoDI6rWNxb1j/efsQXPoCldDKKdcijKhCjBCg2REqQ=='
ENCODING_KEY = 'XN8k4WOG6Hcfyvgh2FPeLeWfM97FH7XoMLNY81YJU8K9WoDI6rWNxb1j%2FefsQXPoCldDKKdcijKhCjBCg2REqQ%3D%3D'

# 어제 날짜로 테스트
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

print(f"테스트 날짜: {yesterday}")
print(f"요청 URL: {API_BASE_URL}")
print("\n" + "="*80)

# 1. 디코딩 키 테스트
print("1️⃣ 디코딩 키 테스트")
print("="*80)

params1 = {
    'serviceKey': DECODING_KEY,
    'numOfRows': 10,
    'pageNo': 1,
    'resultType': 'json',
    'basDt': yesterday
}

try:
    response = requests.get(API_BASE_URL, params=params1, timeout=30)
    print(f"응답 상태: {response.status_code}")

    if response.status_code == 200:
        try:
            data = response.json()
            print("✅ JSON 파싱 성공!")

            if 'response' in data:
                header = data['response'].get('header', {})
                print(f"결과코드: {header.get('resultCode')}")
                print(f"결과메시지: {header.get('resultMsg')}")

                body = data['response'].get('body', {})
                total_count = body.get('totalCount', 0)
                print(f"전체 데이터 수: {total_count}")

                if total_count > 0:
                    items = body.get('items', {}).get('item', [])
                    if items:
                        print(f"\n✅ 디코딩 키 성공! - 첫 번째 종목:")
                        first_item = items[0] if isinstance(items, list) else items
                        print(f"  종목명: {first_item.get('itmsNm')}")
                        print(f"  종목코드: {first_item.get('srtnCd')}")
                        print(f"  종가: {first_item.get('clpr')}")
        except:
            print(f"응답 내용: {response.text[:500]}")
    else:
        print(f"❌ 실패: {response.text[:200]}")
except Exception as e:
    print(f"❌ 오류: {e}")

print("\n" + "="*80)

# 2. 인코딩 키 테스트
print("2️⃣ 인코딩 키 테스트")
print("="*80)

params2 = {
    'serviceKey': ENCODING_KEY,
    'numOfRows': 10,
    'pageNo': 1,
    'resultType': 'json',
    'basDt': yesterday
}

try:
    response = requests.get(API_BASE_URL, params=params2, timeout=30)
    print(f"응답 상태: {response.status_code}")

    if response.status_code == 200:
        try:
            data = response.json()
            print("✅ JSON 파싱 성공!")

            if 'response' in data:
                header = data['response'].get('header', {})
                print(f"결과코드: {header.get('resultCode')}")
                print(f"결과메시지: {header.get('resultMsg')}")

                body = data['response'].get('body', {})
                total_count = body.get('totalCount', 0)
                print(f"전체 데이터 수: {total_count}")

                if total_count > 0:
                    items = body.get('items', {}).get('item', [])
                    if items:
                        print(f"\n✅ 인코딩 키 성공! - 첫 번째 종목:")
                        first_item = items[0] if isinstance(items, list) else items
                        print(f"  종목명: {first_item.get('itmsNm')}")
                        print(f"  종목코드: {first_item.get('srtnCd')}")
                        print(f"  종가: {first_item.get('clpr')}")
        except:
            print(f"응답 내용: {response.text[:500]}")
    else:
        print(f"❌ 실패: {response.text[:200]}")
except Exception as e:
    print(f"❌ 오류: {e}")

print("\n" + "="*80)
print("테스트 완료")
