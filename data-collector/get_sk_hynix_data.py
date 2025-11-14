#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART API를 사용하여 SK하이닉스 재무 데이터 조회
"""

import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()

DART_API_KEY = os.getenv('DART_API_KEY')
SK_HYNIX_CORP_CODE = '00164779'  # SK하이닉스 고유번호
SK_HYNIX_STOCK_CODE = '000660'

def get_latest_quarter():
    """현재 시점 기준 최신 분기 결정"""
    now = datetime.now()
    year = now.year
    month = now.month

    # 분기별 공시 가능 시점 (대략적)
    # 1분기(Q1): 5월 중순 이후
    # 2분기(Q2): 8월 중순 이후
    # 3분기(Q3): 11월 중순 이후
    # 4분기(연간): 다음년도 3월 말 이후

    if month >= 11:
        return year, 3, '11013'  # 3분기
    elif month >= 8:
        return year, 2, '11012'  # 2분기
    elif month >= 5:
        return year, 1, '11011'  # 1분기
    else:
        return year - 1, 4, '11011'  # 전년도 연간보고서

def get_financial_statement(corp_code, year, reprt_code):
    """재무제표 조회"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': corp_code,
        'bsns_year': year,
        'reprt_code': reprt_code
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()

        if data.get('status') == '000':
            return data.get('list', [])
        else:
            print(f"API 오류: {data.get('message', '알 수 없음')}")
            return []
    except Exception as e:
        print(f"요청 실패: {e}")
        return []

def extract_financial_data(statement_list):
    """재무제표에서 필요한 데이터 추출"""
    result = {
        'eps': None,
        'bps': None,
        'net_income': None,
        'total_equity': None,
        'shares_outstanding': None
    }

    for item in statement_list:
        account_nm = item.get('account_nm', '')
        thstrm_amount = item.get('thstrm_amount', '')

        # 금액을 숫자로 변환 (백만원 단위)
        try:
            amount = int(thstrm_amount.replace(',', '')) if thstrm_amount else None
        except:
            amount = None

        # 주요 지표 매핑
        if '주당순이익' in account_nm or 'EPS' in account_nm:
            result['eps'] = amount
        elif '주당순자산' in account_nm or 'BPS' in account_nm:
            result['bps'] = amount
        elif '당기순이익' in account_nm and '지배' in account_nm:
            # 지배기업 소유주 당기순이익
            result['net_income'] = amount
        elif account_nm == '자본총계':
            result['total_equity'] = amount

    return result

def get_stock_price(stock_code, date_str):
    """특정 날짜의 주가 조회 (공공데이터포털 API)"""
    api_key = os.getenv('DATA_GO_KR_API_KEY')
    url = 'http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'

    params = {
        'serviceKey': api_key,
        'numOfRows': '10',
        'pageNo': '1',
        'resultType': 'json',
        'basDt': date_str,
        'srtnCd': stock_code
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()

        if 'response' in data and 'body' in data['response']:
            items = data['response']['body'].get('items', {}).get('item', [])
            if isinstance(items, list) and len(items) > 0:
                item = items[0]
            elif isinstance(items, dict):
                item = items
            else:
                return None, None, None

            return (
                int(item.get('clpr', 0)),  # 종가
                int(item.get('mrktTotAmt', 0)),  # 시가총액
                int(item.get('lstgStCnt', 0))  # 상장주식수
            )
    except Exception as e:
        print(f"주가 조회 실패: {e}")

    return None, None, None

def calculate_per_pbr(stock_price, eps, bps):
    """PER, PBR 계산"""
    per = None
    pbr = None

    if stock_price and eps and eps != 0:
        per = round(stock_price / eps, 2)

    if stock_price and bps and bps != 0:
        pbr = round(stock_price / bps, 2)

    return per, pbr

def main():
    print("=" * 70)
    print("SK하이닉스 재무 데이터 조회 (DART 기준)")
    print("=" * 70)
    print()

    if not DART_API_KEY:
        print("❌ DART_API_KEY가 설정되지 않았습니다.")
        return

    # 1. 어제 날짜 계산
    yesterday = datetime.now() - timedelta(days=1)

    # 주말이면 금요일로 조정
    while yesterday.weekday() >= 5:  # 5=토요일, 6=일요일
        yesterday -= timedelta(days=1)

    date_str = yesterday.strftime('%Y%m%d')
    date_formatted = yesterday.strftime('%Y-%m-%d')

    print(f"📅 조회 날짜: {date_formatted} ({yesterday.strftime('%A')})")
    print()

    # 2. 어제 주가 및 시가총액 조회
    print("📊 주가 데이터 조회 중...")
    stock_price, market_cap, shares_outstanding = get_stock_price(SK_HYNIX_STOCK_CODE, date_str)

    if stock_price:
        print(f"  ✅ 종가: {stock_price:,}원")
        print(f"  ✅ 시가총액: {market_cap:,}백만원 ({market_cap / 1_000_000:,.0f}조원)")
        print(f"  ✅ 상장주식수: {shares_outstanding:,}주")
    else:
        print("  ❌ 주가 데이터를 가져올 수 없습니다.")
        return

    print()

    # 3. 최신 분기 결정
    year, quarter, reprt_code = get_latest_quarter()
    quarter_names = {1: '1분기', 2: '2분기', 3: '3분기', 4: '연간'}

    print(f"📈 재무제표 조회 중... ({year}년 {quarter_names.get(quarter, '연간')})")

    # 4. 재무제표 조회
    statement = get_financial_statement(SK_HYNIX_CORP_CODE, year, reprt_code)

    if not statement:
        print("  ❌ 재무제표를 가져올 수 없습니다.")
        return

    # 5. 재무 데이터 추출
    financial_data = extract_financial_data(statement)

    print(f"  ✅ 재무제표 조회 완료 ({len(statement)}개 항목)")
    print()

    # 6. EPS, BPS 계산
    print("📊 DART 재무 데이터:")

    # EPS 계산: 당기순이익 / 상장주식수
    eps = None
    if financial_data['net_income'] and shares_outstanding:
        # 당기순이익(백만원) * 1,000,000 / 상장주식수 = 주당순이익(원)
        eps = (financial_data['net_income'] * 1_000_000) / shares_outstanding
        print(f"  - 당기순이익: {financial_data['net_income']:,}백만원")
        print(f"  - 상장주식수: {shares_outstanding:,}주")
        print(f"  - EPS (주당순이익): {eps:,.0f}원 (계산됨)")
    else:
        print(f"  - EPS (주당순이익): 계산 불가 (당기순이익 데이터 없음)")

    # BPS 계산: 자본총계 / 상장주식수
    bps = None
    if financial_data['total_equity'] and shares_outstanding:
        # 자본총계(백만원) * 1,000,000 / 상장주식수 = 주당순자산(원)
        bps = (financial_data['total_equity'] * 1_000_000) / shares_outstanding
        print(f"  - 자본총계: {financial_data['total_equity']:,}백만원")
        print(f"  - BPS (주당순자산): {bps:,.0f}원 (계산됨)")
    else:
        print(f"  - BPS (주당순자산): 계산 불가 (자본총계 데이터 없음)")

    print()

    # 7. PER, PBR 계산
    per, pbr = calculate_per_pbr(stock_price, eps, bps)

    print("=" * 70)
    print("📊 최종 결과 (DART 기준)")
    print("=" * 70)
    print(f"종목명: SK하이닉스 ({SK_HYNIX_STOCK_CODE})")
    print(f"기준일: {date_formatted}")
    print(f"재무제표 기준: {year}년 {quarter_names.get(quarter, '연간')}")
    print()
    print(f"✅ 시가총액: {market_cap:,}백만원 ({market_cap / 1_000_000:,.2f}조원)")
    print(f"✅ 주가: {stock_price:,}원")

    if per:
        print(f"✅ PER (주가수익비율): {per:.2f}배")
    else:
        print("❌ PER: 계산 불가 (EPS 데이터 없음)")

    if pbr:
        print(f"✅ PBR (주가순자산비율): {pbr:.2f}배")
    else:
        print("❌ PBR: 계산 불가 (BPS 데이터 없음)")

    print("=" * 70)

if __name__ == '__main__':
    main()
