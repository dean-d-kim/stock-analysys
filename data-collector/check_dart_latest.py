#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART API로 삼성전자와 SK하이닉스의 최신 실적 데이터 확인
"""

import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

DART_API_KEY = os.getenv('DART_API_KEY', '')

def check_latest_report(corp_code, corp_name):
    """최신 공시 보고서 확인"""

    if not DART_API_KEY:
        print(f"❌ DART_API_KEY가 설정되지 않았습니다")
        return

    print(f"\n{'='*80}")
    print(f"📊 {corp_name} 최신 실적 보고서 확인")
    print(f"{'='*80}")

    # 2024년 분기보고서 조회
    for year in [2024]:
        for quarter in [3, 2, 1]:  # 3분기부터 역순으로
            reprt_code = f'110{quarter:02d}'  # 11011(1분기), 11012(2분기), 11013(3분기)

            url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
            params = {
                'crtfc_key': DART_API_KEY,
                'corp_code': corp_code,
                'bsns_year': year,
                'reprt_code': reprt_code
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                data = response.json()

                status = data.get('status', 'error')
                message = data.get('message', '')

                if status == '000':
                    print(f"\n✅ {year}년 {quarter}분기 보고서 존재")
                    print(f"   상태: {status} - {message}")

                    # 데이터 샘플 출력
                    if 'list' in data and len(data['list']) > 0:
                        print(f"   데이터 건수: {len(data['list'])}건")
                        # 주요 계정 찾기
                        for item in data['list'][:10]:
                            account_nm = item.get('account_nm', '')
                            thstrm_amount = item.get('thstrm_amount', '0')
                            if account_nm in ['매출액', '당기순이익', '영업이익']:
                                print(f"   - {account_nm}: {thstrm_amount}")
                    return  # 최신 데이터를 찾았으므로 종료

                elif status == '013':
                    print(f"⚠️  {year}년 {quarter}분기: 데이터 없음 ({message})")
                else:
                    print(f"❌ {year}년 {quarter}분기: {status} - {message}")

            except Exception as e:
                print(f"❌ {year}년 {quarter}분기 조회 실패: {e}")
                continue

def main():
    print("="*80)
    print("🔍 DART 최신 실적 데이터 확인")
    print("="*80)

    # 삼성전자
    check_latest_report('00126380', '삼성전자')

    # SK하이닉스
    check_latest_report('00164779', 'SK하이닉스')

    print("\n" + "="*80)
    print("참고: DART 분기보고서는 통상 분기 종료 후 45일 이내 공시됩니다")
    print("- 1분기(1~3월): 5월 중순까지")
    print("- 2분기(4~6월): 8월 중순까지")
    print("- 3분기(7~9월): 11월 중순까지")
    print("="*80)

if __name__ == "__main__":
    main()
