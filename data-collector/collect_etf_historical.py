#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공공데이터포털 ETF 과거 데이터 수집 (2020년 ~ 현재)
"""

import psycopg2
import os
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import sys

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)

# API 설정
API_BASE_URL = 'https://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService/getETFPriceInfo'
API_KEY = os.getenv('DATA_GO_KR_API_KEY')

# DB 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

def get_db_connection():
    """DB 연결"""
    return psycopg2.connect(**DB_CONFIG)

def get_etf_price_data(base_date, page_no=1, num_of_rows=1000):
    """
    공공데이터포털 API로 ETF 시세 데이터 조회
    """
    params = {
        'serviceKey': API_KEY,
        'pageNo': page_no,
        'numOfRows': num_of_rows,
        'resultType': 'json',
        'basDt': base_date
    }

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # 응답 구조 확인
        if 'response' in data and 'body' in data['response']:
            body = data['response']['body']
            items = body.get('items', {})

            if isinstance(items, dict):
                item_list = items.get('item', [])
            elif isinstance(items, list):
                item_list = items
            else:
                item_list = []

            if not isinstance(item_list, list):
                item_list = [item_list] if item_list else []

            total_count = body.get('totalCount', 0)

            return item_list, total_count
        else:
            return [], 0

    except Exception as e:
        logging.error(f"  ❌ API 호출 실패: {e}")
        return [], 0

def save_etf_info(etf_list):
    """ETF 종목 정보 저장"""
    if not etf_list:
        return 0

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        inserted = 0
        for etf in etf_list:
            stock_code = etf.get('srtnCd', '')
            stock_name = etf.get('itmsNm', '')

            if not stock_code or not stock_name:
                continue

            # ETF 정보 저장 (asset_type='ETF'로 설정)
            cur.execute("""
                INSERT INTO stocks (stock_code, stock_name, market_type, asset_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stock_code) DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    asset_type = EXCLUDED.asset_type
            """, (stock_code, stock_name, 'KRX', 'ETF'))

            inserted += cur.rowcount

        conn.commit()
        return inserted

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"  ❌ 종목 정보 저장 실패: {e}")
        return 0
    finally:
        if conn:
            cur.close()
            conn.close()

def save_etf_prices(prices_data, trade_date):
    """ETF 가격 데이터 저장"""
    if not prices_data:
        return 0

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        inserted = 0
        for etf in prices_data:
            stock_code = etf.get('srtnCd', '')

            # OHLCV 데이터
            try:
                open_price = int(float(etf.get('mkp', 0)))
                high_price = int(float(etf.get('hipr', 0)))
                low_price = int(float(etf.get('lopr', 0)))
                close_price = int(float(etf.get('clpr', 0)))
                volume = int(float(etf.get('trqu', 0)))
            except (ValueError, TypeError):
                continue

            if close_price == 0:
                continue

            # 추가 필드
            vs = None
            change_rate = None
            trading_value = None

            try:
                vs_str = etf.get('vs', '')
                if vs_str and vs_str != '':
                    vs = int(float(vs_str))
            except (ValueError, TypeError):
                pass

            try:
                change_rate_str = etf.get('fltRt', '')
                if change_rate_str and change_rate_str != '':
                    change_rate = float(change_rate_str)
            except (ValueError, TypeError):
                pass

            try:
                trading_value_str = etf.get('trPrc', '')
                if trading_value_str and trading_value_str != '':
                    trading_value = int(float(trading_value_str))
            except (ValueError, TypeError):
                pass

            cur.execute("""
                INSERT INTO daily_prices (
                    stock_code, trade_date, open_price, high_price, low_price, close_price, volume,
                    vs, change_rate, trading_value
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
            """, (stock_code, trade_date, open_price, high_price, low_price, close_price, volume,
                  vs, change_rate, trading_value))

            inserted += cur.rowcount

        conn.commit()
        return inserted

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"  ❌ 가격 데이터 저장 실패: {e}")
        return 0
    finally:
        if conn:
            cur.close()
            conn.close()

def collect_single_date(date_str):
    """특정 날짜의 ETF 데이터 수집"""
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    trade_date = date_obj.strftime('%Y-%m-%d')

    # API 호출
    data, total_count = get_etf_price_data(date_str)

    if not data:
        return 0

    # 종목 정보 저장
    save_etf_info(data)

    # 가격 데이터 저장
    saved = save_etf_prices(data, trade_date)

    return saved

def collect_date_range(start_date_str, end_date_str):
    """날짜 범위의 데이터 수집"""
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')

    current_date = start_date
    total_records = 0
    day_count = 0
    total_days = (end_date - start_date).days + 1

    while current_date <= end_date:
        day_count += 1
        date_str = current_date.strftime('%Y%m%d')

        logging.info(f"\n{'='*80}")
        logging.info(f"[{day_count}/{total_days}] {current_date.strftime('%Y-%m-%d')} 데이터 수집")
        logging.info(f"{'='*80}")

        saved = collect_single_date(date_str)

        if saved > 0:
            logging.info(f"  ✅ {saved}건 저장 완료")
            total_records += saved
        else:
            logging.warning(f"  ⚠️  데이터 없음")

        current_date += timedelta(days=1)
        time.sleep(0.5)  # API 호출 간 대기

    return total_records

def main():
    logging.info("="*80)
    logging.info("🚀 ETF 과거 데이터 수집 (2020년 ~ 현재)")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not API_KEY:
        logging.error("❌ DATA_GO_KR_API_KEY가 설정되지 않았습니다.")
        return

    # 수집 기간 설정: 2020-01-01 ~ 현재
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()

    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    logging.info(f"📅 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    total_days = (end_date - start_date).days + 1
    logging.info(f"📆 총 {total_days}일")

    # 데이터 수집
    total_records = collect_date_range(start_str, end_str)

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ ETF 과거 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 레코드 수: {total_records:,}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
