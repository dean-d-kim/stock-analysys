#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2025년 2분기 삼성전자 재무제표 조회
"""

import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime

load_dotenv()

DART_API_KEY = os.getenv('DART_API_KEY', '')
SAMSUNG_CORP_CODE = '00126380'  # 삼성전자 고유번호

def get_financial_statement(year=2025, quarter=2):
    """
    삼성전자 재무제표 조회

    Args:
        year: 사업연도 (기본: 2025)
        quarter: 분기 (1, 2, 3, 4) (기본: 2)
    """

    if not DART_API_KEY:
        print("❌ DART_API_KEY가 설정되지 않았습니다")
        print("   .env 파일에 DART_API_KEY를 설정해주세요")
        return None

    # 보고서 코드 매핑
    report_codes = {
        1: '11011',  # 1분기보고서
        2: '11012',  # 2분기보고서 (반기보고서)
        3: '11013',  # 3분기보고서
        4: '11014'   # 사업보고서 (연간)
    }

    reprt_code = report_codes.get(quarter)
    if not reprt_code:
        print(f"❌ 잘못된 분기 번호: {quarter} (1~4만 가능)")
        return None

    print("="*80)
    print(f"📊 삼성전자 {year}년 {quarter}분기 재무제표 조회")
    print("="*80)

    # DART API 호출
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': SAMSUNG_CORP_CODE,
        'bsns_year': year,
        'reprt_code': reprt_code
    }

    try:
        print(f"\n🔍 API 호출 중...")
        print(f"   URL: {url}")
        print(f"   기업코드: {SAMSUNG_CORP_CODE}")
        print(f"   사업연도: {year}")
        print(f"   보고서코드: {reprt_code} ({quarter}분기)")

        response = requests.get(url, params=params, timeout=30)
        data = response.json()

        status = data.get('status')
        message = data.get('message')

        print(f"\n📡 API 응답:")
        print(f"   상태코드: {status}")
        print(f"   메시지: {message}")

        if status == '000':
            # 성공
            print(f"\n✅ 데이터 조회 성공!")

            if 'list' not in data or len(data['list']) == 0:
                print("   ⚠️  조회된 데이터가 없습니다")
                return None

            financial_data = data['list']
            print(f"   총 {len(financial_data)}건의 계정 데이터 조회됨")

            # 주요 재무지표 추출
            extract_key_metrics(financial_data, year, quarter)

            return financial_data

        elif status == '013':
            print(f"\n⚠️  해당 보고서가 아직 공시되지 않았습니다")
            print(f"   {year}년 {quarter}분기 보고서는 아직 제출되지 않았을 수 있습니다")
            print(f"\n   📅 예상 공시 일정:")

            if quarter == 1:
                print(f"      1분기 (1~3월): {year}년 5월 중순까지")
            elif quarter == 2:
                print(f"      2분기 (4~6월): {year}년 8월 중순까지")
            elif quarter == 3:
                print(f"      3분기 (7~9월): {year}년 11월 중순까지")
            else:
                print(f"      사업보고서: {year+1}년 3월 중순까지")

            return None

        else:
            print(f"\n❌ API 오류 발생")
            print(f"   상태: {status}")
            print(f"   메시지: {message}")
            return None

    except requests.exceptions.Timeout:
        print("\n❌ API 요청 시간 초과")
        return None
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return None

def extract_key_metrics(financial_data, year, quarter):
    """주요 재무지표 추출 및 출력"""

    print("\n" + "="*80)
    print(f"💰 주요 재무지표 ({year}년 {quarter}분기)")
    print("="*80)

    # 주요 계정 과목
    key_accounts = {
        '매출액': None,
        '영업이익': None,
        '당기순이익': None,
        '자산총계': None,
        '부채총계': None,
        '자본총계': None,
        '유동자산': None,
        '비유동자산': None,
        '유동부채': None,
        '비유동부채': None
    }

    # 데이터에서 주요 계정 찾기
    for item in financial_data:
        account_nm = item.get('account_nm', '')
        thstrm_amount = item.get('thstrm_amount', '0')  # 당기금액

        if account_nm in key_accounts:
            try:
                # 쉼표 제거 후 숫자로 변환
                amount = int(thstrm_amount.replace(',', ''))
                key_accounts[account_nm] = amount
            except:
                key_accounts[account_nm] = thstrm_amount

    # 출력
    print("\n📈 손익계산서 (단위: 억원)")
    print("-" * 80)
    for account in ['매출액', '영업이익', '당기순이익']:
        value = key_accounts.get(account)
        if value and isinstance(value, int):
            # 백만원 단위를 억원으로 변환 (DART는 백만원 단위)
            value_in_billion = value / 100
            print(f"  {account:15s}: {value_in_billion:>15,.0f} 억원")
        else:
            print(f"  {account:15s}: {str(value):>15s}")

    print("\n📊 재무상태표 (단위: 억원)")
    print("-" * 80)
    for account in ['자산총계', '부채총계', '자본총계']:
        value = key_accounts.get(account)
        if value and isinstance(value, int):
            value_in_billion = value / 100
            print(f"  {account:15s}: {value_in_billion:>15,.0f} 억원")
        else:
            print(f"  {account:15s}: {str(value):>15s}")

    # 재무비율 계산
    print("\n📉 주요 재무비율")
    print("-" * 80)

    if key_accounts['영업이익'] and key_accounts['매출액']:
        if isinstance(key_accounts['영업이익'], int) and isinstance(key_accounts['매출액'], int):
            operating_margin = (key_accounts['영업이익'] / key_accounts['매출액']) * 100
            print(f"  영업이익률:        {operating_margin:>15.2f} %")

    if key_accounts['당기순이익'] and key_accounts['매출액']:
        if isinstance(key_accounts['당기순이익'], int) and isinstance(key_accounts['매출액'], int):
            net_margin = (key_accounts['당기순이익'] / key_accounts['매출액']) * 100
            print(f"  순이익률:          {net_margin:>15.2f} %")

    if key_accounts['부채총계'] and key_accounts['자본총계']:
        if isinstance(key_accounts['부채총계'], int) and isinstance(key_accounts['자본총계'], int):
            debt_ratio = (key_accounts['부채총계'] / key_accounts['자본총계']) * 100
            print(f"  부채비율:          {debt_ratio:>15.2f} %")

    print("="*80)

def save_to_json(financial_data, year, quarter):
    """재무제표 데이터를 JSON 파일로 저장"""

    if not financial_data:
        print("\n⚠️  저장할 데이터가 없습니다")
        return

    filename = f"samsung_{year}_Q{quarter}_financial.json"

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(financial_data, f, ensure_ascii=False, indent=2)

        print(f"\n💾 데이터 저장 완료: {filename}")
        print(f"   총 {len(financial_data)}건의 계정 데이터 저장됨")

    except Exception as e:
        print(f"\n❌ 파일 저장 실패: {e}")

def main():
    """메인 실행 함수"""

    print("\n" + "="*80)
    print("🏢 삼성전자 재무제표 조회 프로그램")
    print("="*80)
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 2025년 2분기 데이터 조회
    year = 2025
    quarter = 2

    financial_data = get_financial_statement(year, quarter)

    # JSON 파일로 저장 (자동)
    if financial_data:
        save_to_json(financial_data, year, quarter)

    print("\n" + "="*80)
    print("✅ 프로그램 종료")
    print("="*80)

if __name__ == "__main__":
    main()
