#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공공데이터포털 API 전체 히스토리 데이터 수집 (1990년 ~ 현재)
주식 및 ETF 모든 데이터 저장
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
STOCK_API_URL = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'
ETF_API_URL = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getETFPriceInfo'
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

def get_api_data(api_url, base_date, page_no=1, num_of_rows=1000):
    """
    공공데이터포털 API로 데이터 조회

    Args:
        api_url: API URL
        base_date: 기준일자 (YYYYMMDD)
        page_no: 페이지 번호
        num_of_rows: 한 페이지 결과 수

    Returns:
        list: 데이터 목록
    """
    params = {
        'serviceKey': API_KEY,
        'numOfRows': num_of_rows,
        'pageNo': page_no,
        'resultType': 'json',
        'basDt': base_date
    }

    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        if 'response' in data and 'body' in data['response']:
            body = data['response']['body']
            total_count = body.get('totalCount', 0)

            if total_count == 0:
                return []

            items = body.get('items', {}).get('item', [])

            if isinstance(items, dict):
                items = [items]

            return items
        else:
            logging.error(f"  ❌ API 응답 오류: {data}")
            return []

    except requests.exceptions.Timeout:
        logging.error(f"  ❌ API 타임아웃: {base_date}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"  ❌ API 요청 실패: {e}")
        return []
    except Exception as e:
        logging.error(f"  ❌ 데이터 처리 실패: {e}")
        return []

def insert_stock_batch(stocks_data):
    """종목 정보 배치 삽입"""
    if not stocks_data:
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for stock in stocks_data:
            stock_code = stock.get('srtnCd', '')
            stock_name = stock.get('itmsNm', '')
            market_type = stock.get('mrktCtg', '')

            # 추가 정보
            isin_code = stock.get('isinCd', None)
            listed_shares = None
            market_cap = None

            # 상장주식수
            try:
                listed_shares_str = stock.get('lstgStCnt', '')
                if listed_shares_str and listed_shares_str != '':
                    listed_shares = int(float(listed_shares_str))
            except (ValueError, TypeError):
                pass

            # 시가총액
            try:
                market_cap_str = stock.get('mrktTotAmt', '')
                if market_cap_str and market_cap_str != '':
                    market_cap = int(float(market_cap_str))
            except (ValueError, TypeError):
                pass

            # 시장 구분
            if market_type == 'KOSPI':
                market_type = 'KOSPI'
            elif market_type == 'KOSDAQ':
                market_type = 'KOSDAQ'
            elif market_type == 'KONEX':
                market_type = 'ETF'
            else:
                market_type = 'ETC'

            cur.execute("""
                INSERT INTO stocks (
                    stock_code, stock_name, market_type, asset_type,
                    isin_code, listed_shares, market_cap
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code)
                DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    market_type = EXCLUDED.market_type,
                    isin_code = EXCLUDED.isin_code,
                    listed_shares = EXCLUDED.listed_shares,
                    market_cap = EXCLUDED.market_cap
            """, (stock_code, stock_name, market_type, 'STOCK',
                  isin_code, listed_shares, market_cap))

        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"  ❌ 종목 정보 저장 실패: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_etf_batch(etfs_data):
    """ETF 정보 배치 삽입"""
    if not etfs_data:
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for etf in etfs_data:
            stock_code = etf.get('srtnCd', '')
            stock_name = etf.get('itmsNm', '')

            # 추가 정보
            isin_code = etf.get('isinCd', None)
            listed_shares = None
            nav = None
            net_asset_total = None
            base_index_name = etf.get('idxNm', None)
            base_index_close = None

            # 상장좌수
            try:
                listed_shares_str = etf.get('lstgStCnt', '')
                if listed_shares_str and listed_shares_str != '':
                    listed_shares = int(float(listed_shares_str))
            except (ValueError, TypeError):
                pass

            # NAV
            try:
                nav_str = etf.get('nav', '')
                if nav_str and nav_str != '':
                    nav = float(nav_str)
            except (ValueError, TypeError):
                pass

            # 순자산총액
            try:
                net_asset_str = etf.get('lstgAmt', '')
                if net_asset_str and net_asset_str != '':
                    net_asset_total = int(float(net_asset_str))
            except (ValueError, TypeError):
                pass

            # 기초지수종가
            try:
                idx_close_str = etf.get('idxCsf', '')
                if idx_close_str and idx_close_str != '':
                    base_index_close = float(idx_close_str)
            except (ValueError, TypeError):
                pass

            cur.execute("""
                INSERT INTO stocks (
                    stock_code, stock_name, market_type, asset_type,
                    isin_code, listed_shares, nav, net_asset_total,
                    base_index_name, base_index_close
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code)
                DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    market_type = EXCLUDED.market_type,
                    asset_type = EXCLUDED.asset_type,
                    isin_code = EXCLUDED.isin_code,
                    listed_shares = EXCLUDED.listed_shares,
                    nav = EXCLUDED.nav,
                    net_asset_total = EXCLUDED.net_asset_total,
                    base_index_name = EXCLUDED.base_index_name,
                    base_index_close = EXCLUDED.base_index_close
            """, (stock_code, stock_name, 'ETF', 'ETF',
                  isin_code, listed_shares, nav, net_asset_total,
                  base_index_name, base_index_close))

        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"  ❌ ETF 정보 저장 실패: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_daily_price_batch(prices_data, trade_date):
    """일별 시세 배치 삽입"""
    if not prices_data:
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        inserted = 0
        for item in prices_data:
            stock_code = item.get('srtnCd', '')

            # OHLCV 데이터
            try:
                open_price = int(float(item.get('mkp', 0)))
                high_price = int(float(item.get('hipr', 0)))
                low_price = int(float(item.get('lopr', 0)))
                close_price = int(float(item.get('clpr', 0)))
                volume = int(float(item.get('trqu', 0)))
            except (ValueError, TypeError):
                continue

            if close_price == 0:
                continue

            # 추가 필드
            vs = None
            change_rate = None
            trading_value = None

            try:
                vs_str = item.get('vs', '')
                if vs_str and vs_str != '':
                    vs = int(float(vs_str))
            except (ValueError, TypeError):
                pass

            try:
                change_rate_str = item.get('fltRt', '')
                if change_rate_str and change_rate_str != '':
                    change_rate = float(change_rate_str)
            except (ValueError, TypeError):
                pass

            try:
                trading_value_str = item.get('trPrc', '')
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

def collect_date_data(date_str, date_formatted):
    """특정 날짜의 데이터 수집"""
    total_records = 0

    # 1. 주식 데이터 수집
    logging.info(f"  📊 주식 데이터 조회 중...")
    page_no = 1
    all_stock_items = []

    while True:
        items = get_api_data(STOCK_API_URL, date_str, page_no=page_no, num_of_rows=1000)

        if not items:
            break

        all_stock_items.extend(items)

        if len(items) < 1000:
            break

        page_no += 1
        time.sleep(0.3)

    if all_stock_items:
        insert_stock_batch(all_stock_items)
        count = insert_daily_price_batch(all_stock_items, date_formatted)
        logging.info(f"  ✅ 주식 {count}건 저장")
        total_records += count

    # 2. ETF 데이터 수집
    logging.info(f"  📊 ETF 데이터 조회 중...")
    page_no = 1
    all_etf_items = []

    while True:
        items = get_api_data(ETF_API_URL, date_str, page_no=page_no, num_of_rows=1000)

        if not items:
            break

        all_etf_items.extend(items)

        if len(items) < 1000:
            break

        page_no += 1
        time.sleep(0.3)

    if all_etf_items:
        insert_etf_batch(all_etf_items)
        count = insert_daily_price_batch(all_etf_items, date_formatted)
        logging.info(f"  ✅ ETF {count}건 저장")
        total_records += count

    return total_records

def collect_full_historical(start_date, end_date):
    """
    전체 기간 히스토리 데이터 수집

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
    """
    current_date = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')

    total_days = (end - current_date).days + 1
    processed_days = 0
    total_records = 0
    success_days = 0

    logging.info(f"\n{'='*80}")
    logging.info(f"📅 수집 기간: {current_date.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}")
    logging.info(f"   총 {total_days:,}일 처리 예정")
    logging.info(f"{'='*80}\n")

    while current_date <= end:
        date_str = current_date.strftime('%Y%m%d')
        date_formatted = current_date.strftime('%Y-%m-%d')

        processed_days += 1

        # 주말은 스킵 (토요일=5, 일요일=6)
        if current_date.weekday() >= 5:
            logging.info(f"[{processed_days:,}/{total_days:,}] {date_formatted} - 주말 스킵")
            current_date += timedelta(days=1)
            continue

        logging.info(f"\n{'='*80}")
        logging.info(f"[{processed_days:,}/{total_days:,}] {date_formatted} 데이터 수집")
        logging.info(f"{'='*80}")

        try:
            records = collect_date_data(date_str, date_formatted)

            if records > 0:
                total_records += records
                success_days += 1
                logging.info(f"  ✅ 총 {records}건 저장 완료")
            else:
                logging.info(f"  ℹ️  데이터 없음 (휴장일 가능)")

        except Exception as e:
            logging.error(f"  ❌ 오류 발생: {e}")

        # 진행률 표시
        progress = (processed_days / total_days) * 100
        logging.info(f"\n📊 진행률: {progress:.1f}% ({processed_days:,}/{total_days:,}일)")
        logging.info(f"   누적 레코드: {total_records:,}건")
        logging.info(f"   성공한 날: {success_days:,}일")

        current_date += timedelta(days=1)
        time.sleep(1)  # API 제한 방지

    return total_records, success_days

def main():
    logging.info("="*80)
    logging.info("🚀 공공데이터포털 API 전체 히스토리 데이터 수집")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not API_KEY:
        logging.error("❌ DATA_GO_KR_API_KEY가 설정되지 않았습니다.")
        return

    # 2020년 1월 1일부터 현재까지 (공공데이터포털 API는 2020년부터 제공)
    start_date = '20200101'
    end_date = datetime.now().strftime('%Y%m%d')

    # 데이터 수집
    total_records, success_days = collect_full_historical(start_date, end_date)

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 전체 히스토리 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 레코드 수: {total_records:,}건")
    logging.info(f"성공한 날: {success_days:,}일")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
