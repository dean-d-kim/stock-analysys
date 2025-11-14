#!/usr/bin/env python3
"""
2025년 주식 데이터 재수집 스크립트
공공데이터포털 API 사용
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2

# .env 파일 로드
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

# 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

API_KEY = os.getenv('DATA_GO_KR_API_KEY')
BASE_URL = 'http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService'

def get_db_connection():
    """데이터베이스 연결"""
    return psycopg2.connect(**DB_CONFIG)

def get_business_days_2025():
    """2025년 영업일 목록 생성 (주말 제외)"""
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now()

    business_days = []
    current_date = start_date

    while current_date <= end_date:
        # 주말(토요일=5, 일요일=6) 제외
        if current_date.weekday() < 5:
            business_days.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return business_days

def fetch_stock_data(date_str):
    """특정 날짜의 주식 데이터 조회"""
    url = f"{BASE_URL}/getStockPriceInfo"
    params = {
        'serviceKey': API_KEY,
        'numOfRows': '10000',
        'pageNo': '1',
        'resultType': 'json',
        'basDt': date_str
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'response' in data and 'body' in data['response']:
            items = data['response']['body'].get('items', {}).get('item', [])
            return items if isinstance(items, list) else [items] if items else []
        return []
    except Exception as e:
        print(f"❌ API 오류 ({date_str}): {e}")
        return []

def save_stock_data(conn, date_str, items):
    """주식 데이터 저장"""
    cursor = conn.cursor()
    saved_count = 0

    for item in items:
        try:
            # 종목 정보 UPSERT
            cursor.execute("""
                INSERT INTO stocks (
                    stock_code, stock_name, market_type, asset_type,
                    isin_code, listed_shares, market_cap
                )
                VALUES (%s, %s, %s, 'STOCK', %s, %s, %s)
                ON CONFLICT (stock_code)
                DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    market_type = EXCLUDED.market_type,
                    isin_code = EXCLUDED.isin_code,
                    listed_shares = EXCLUDED.listed_shares,
                    market_cap = EXCLUDED.market_cap
            """, (
                item.get('srtnCd'),
                item.get('itmsNm'),
                item.get('mrktCtg'),
                item.get('isinCd'),
                int(item.get('lstgStCnt', 0)) if item.get('lstgStCnt') else None,
                int(item.get('mrktTotAmt', 0)) if item.get('mrktTotAmt') else None
            ))

            # 일별 시세 UPSERT
            cursor.execute("""
                INSERT INTO daily_prices (
                    stock_code, trade_date, open_price, high_price, low_price,
                    close_price, volume, vs, change_rate, trading_value
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code, trade_date)
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    vs = EXCLUDED.vs,
                    change_rate = EXCLUDED.change_rate,
                    trading_value = EXCLUDED.trading_value
            """, (
                item.get('srtnCd'),
                date_str[:4] + '-' + date_str[4:6] + '-' + date_str[6:8],
                int(item.get('mkp', 0)) if item.get('mkp') else None,
                int(item.get('hipr', 0)) if item.get('hipr') else None,
                int(item.get('lopr', 0)) if item.get('lopr') else None,
                int(item.get('clpr', 0)) if item.get('clpr') else None,
                int(item.get('trqu', 0)) if item.get('trqu') else None,
                int(item.get('vs', 0)) if item.get('vs') else None,
                float(item.get('fltRt', 0)) if item.get('fltRt') else None,
                int(item.get('trPrc', 0)) if item.get('trPrc') else None
            ))

            saved_count += 1

        except Exception as e:
            print(f"⚠️  데이터 저장 오류 ({item.get('srtnCd')}): {e}")
            continue

    conn.commit()
    cursor.close()
    return saved_count

def main():
    print("=" * 60)
    print("2025년 주식 데이터 재수집 시작")
    print("=" * 60)

    # 영업일 목록 생성
    business_days = get_business_days_2025()
    total_days = len(business_days)
    print(f"\n📅 수집 대상: {total_days}개 영업일 (2025-01-01 ~ {datetime.now().strftime('%Y-%m-%d')})")

    # 데이터베이스 연결
    conn = get_db_connection()
    print("✅ 데이터베이스 연결 성공\n")

    # 데이터 수집
    total_saved = 0

    for idx, date_str in enumerate(business_days, 1):
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        print(f"[{idx}/{total_days}] {formatted_date} 데이터 수집 중...", end=' ')

        # API 호출
        items = fetch_stock_data(date_str)

        if items:
            # 데이터 저장
            saved_count = save_stock_data(conn, date_str, items)
            total_saved += saved_count
            print(f"✅ {saved_count}건 저장 (누적: {total_saved:,}건)")
        else:
            print("⚠️  데이터 없음")

        # API 호출 제한 (1초 대기)
        if idx < total_days:
            time.sleep(1)

    conn.close()

    print("\n" + "=" * 60)
    print(f"✅ 수집 완료!")
    print(f"   - 총 영업일: {total_days}일")
    print(f"   - 총 저장 레코드: {total_saved:,}건")
    print("=" * 60)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 오류 발생: {e}")
        sys.exit(1)
