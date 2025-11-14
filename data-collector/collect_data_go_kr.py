#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공공데이터포털 주식시세정보 API를 사용한 데이터 수집
https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService
"""

import psycopg2
import os
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys
import xml.etree.ElementTree as ET
from common.logger import get_logger, log_exception, log_api_call, log_db_operation

load_dotenv()

# 로거 설정 (Phase 3 개선)
logger = get_logger(__name__, 'collect_data_go_kr.log')

# API 설정
API_BASE_URL = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'
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

def get_stock_price_data(base_date, page_no=1, num_of_rows=1000):
    """
    공공데이터포털 API로 주식 시세 데이터 조회

    Args:
        base_date: 기준일자 (YYYYMMDD)
        page_no: 페이지 번호
        num_of_rows: 한 페이지 결과 수

    Returns:
        list: 주식 시세 데이터 목록
    """
    params = {
        'serviceKey': API_KEY,
        'numOfRows': num_of_rows,
        'pageNo': page_no,
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

            # totalCount 확인
            total_count = body.get('totalCount', 0)

            if total_count == 0:
                logger.warning(f"  데이터 없음: {base_date}")
                log_api_call(logger, 'getStockPriceInfo', {'base_date': base_date}, success=True, error="데이터 없음")
                return []

            # items 추출
            items = body.get('items', {}).get('item', [])

            # item이 단일 객체인 경우 리스트로 변환
            if isinstance(items, dict):
                items = [items]

            logger.info(f"  ✅ {len(items)}개 종목 조회 (전체: {total_count}개)")
            log_api_call(logger, 'getStockPriceInfo', {'base_date': base_date, 'count': len(items)}, success=True)
            return items
        else:
            logger.error(f"  ❌ API 응답 오류: {data}")
            log_api_call(logger, 'getStockPriceInfo', {'base_date': base_date}, success=False, error="응답 구조 오류")
            return []

    except requests.exceptions.Timeout:
        logger.error(f"  ❌ API 타임아웃: {base_date}")
        log_api_call(logger, 'getStockPriceInfo', {'base_date': base_date}, success=False, error="Timeout")
        return []
    except requests.exceptions.RequestException as e:
        log_exception(logger, f"  ❌ API 요청 실패: {e}")
        log_api_call(logger, 'getStockPriceInfo', {'base_date': base_date}, success=False, error=str(e))
        return []
    except Exception as e:
        log_exception(logger, f"  ❌ 데이터 처리 실패: {e}")
        log_api_call(logger, 'getStockPriceInfo', {'base_date': base_date}, success=False, error=str(e))
        return []

def insert_stock_batch(stocks_data):
    """종목 정보 배치 삽입 - API의 모든 필드 저장"""
    if not stocks_data:
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for stock in stocks_data:
            # 기본 정보
            stock_code = stock.get('srtnCd', '')  # 단축코드
            stock_name = stock.get('itmsNm', '')  # 종목명
            market_type = stock.get('mrktCtg', '')  # 시장구분

            # 추가 정보
            isin_code = stock.get('isinCd', None)  # ISIN 코드
            listed_shares = None
            market_cap = None

            # 상장주식수 (lstgStCnt)
            try:
                listed_shares_str = stock.get('lstgStCnt', '')
                if listed_shares_str and listed_shares_str != '':
                    listed_shares = int(float(listed_shares_str))
            except (ValueError, TypeError):
                pass

            # 시가총액 (mrktTotAmt) - 원 단위
            try:
                market_cap_str = stock.get('mrktTotAmt', '')
                if market_cap_str and market_cap_str != '':
                    market_cap = int(float(market_cap_str))
            except (ValueError, TypeError):
                pass

            # 시장 구분 매핑 (종목명 기반으로 ETF 판별)
            # ETF는 종목명에 KODEX, TIGER, ARIRANG, KBSTAR 등 특정 ETF 브랜드명이 포함됨
            # KB, NH, MIRAE, KIWOOM 같은 일반 증권사명은 제외 (일반 종목과 혼동)
            etf_keywords = ['KODEX', 'TIGER', 'ARIRANG', 'KBSTAR', 'KOSEF', 'TREX', 'SOL ', 'ACE ',
                           'TIMEFOLIO', 'RISE', 'PLUS', 'HANARO', 'SMART', 'KINDEX', 'SYNTH',
                           'TRUE', 'MULTI', 'FOCUS', 'ITF', 'ALPHA', 'KTOP', 'QV',
                           '1Q', 'HK ', '마이티', '에셋플러스']

            is_etf = any(keyword in stock_name for keyword in etf_keywords)

            if is_etf:
                market_type = 'ETF'
            elif market_type == 'KOSPI':
                market_type = 'KOSPI'
            elif market_type == 'KOSDAQ':
                market_type = 'KOSDAQ'
            elif market_type == 'KONEX':
                market_type = 'KONEX'
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
        logger.info(f"  ✅ {len(stocks_data)}개 종목 정보 저장 (ISIN, 상장주식수, 시가총액 포함)")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"  ❌ 종목 정보 저장 실패: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_daily_price_batch(prices_data, trade_date):
    """일별 시세 배치 삽입 - API의 모든 필드 저장"""
    if not prices_data:
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        inserted = 0
        for stock in prices_data:
            stock_code = stock.get('srtnCd', '')

            # OHLCV 데이터
            open_price = int(stock.get('mkp', 0))  # 시가
            high_price = int(stock.get('hipr', 0))  # 고가
            low_price = int(stock.get('lopr', 0))  # 저가
            close_price = int(stock.get('clpr', 0))  # 종가
            volume = int(stock.get('trqu', 0))  # 거래량

            if close_price == 0:  # 가격이 0인 경우 스킵
                continue

            # 추가 필드
            vs = None  # 전일대비 (vs)
            change_rate = None  # 등락율 (fltRt)
            trading_value = None  # 거래대금 (trPrc)

            try:
                vs_str = stock.get('vs', '')
                if vs_str and vs_str != '':
                    vs = int(float(vs_str))
            except (ValueError, TypeError):
                pass

            try:
                change_rate_str = stock.get('fltRt', '')
                if change_rate_str and change_rate_str != '':
                    change_rate = float(change_rate_str)
            except (ValueError, TypeError):
                pass

            try:
                trading_value_str = stock.get('trPrc', '')
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
        logger.error(f"  ❌ 가격 데이터 저장 실패: {e}")
        return 0
    finally:
        if conn:
            cur.close()
            conn.close()

def collect_date_range(start_date, end_date):
    """
    지정 기간의 데이터 수집

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
    """
    current_date = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')

    total_days = (end - current_date).days + 1
    processed_days = 0
    total_records = 0

    while current_date <= end:
        date_str = current_date.strftime('%Y%m%d')
        date_formatted = current_date.strftime('%Y-%m-%d')

        processed_days += 1
        logger.info(f"\n{'='*80}")
        logger.info(f"[{processed_days}/{total_days}] {date_formatted} 데이터 수집")
        logger.info(f"{'='*80}")

        # 페이지별 조회 (한 번에 최대 1000개)
        page_no = 1
        all_items = []

        while True:
            items = get_stock_price_data(date_str, page_no=page_no, num_of_rows=1000)

            if not items:
                break

            all_items.extend(items)

            # 1000개 미만이면 마지막 페이지
            if len(items) < 1000:
                break

            page_no += 1
            time.sleep(0.5)  # API 제한 방지

        if all_items:
            # 종목 정보 저장
            insert_stock_batch(all_items)

            # 가격 데이터 저장
            count = insert_daily_price_batch(all_items, date_formatted)
            logger.info(f"  ✅ {count}건 가격 데이터 저장")
            total_records += count

        # 다음 날짜로
        current_date += timedelta(days=1)
        time.sleep(1)  # API 제한 방지

    return total_records

def main():
    logger.info("="*80)
    logger.info("🚀 공공데이터포털 API 주식 데이터 수집")
    logger.info("="*80)
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not API_KEY:
        logger.error("❌ DATA_GO_KR_API_KEY가 설정되지 않았습니다.")
        logger.error("  .env 파일에 공공데이터포털 API 키를 추가하세요.")
        return

    # 수집 기간 설정 (최근 30일)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    logger.info(f"📅 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 데이터 수집
    total_records = collect_date_range(start_str, end_str)

    # 최종 요약
    logger.info(f"\n{'='*80}")
    logger.info("✅ 데이터 수집 완료")
    logger.info(f"{'='*80}")
    logger.info(f"총 레코드 수: {total_records}건")
    logger.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*80}")

if __name__ == '__main__':
    main()
