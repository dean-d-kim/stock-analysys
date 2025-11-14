#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
코스피, 코스닥, ETF 상위 200개 종목 데이터 수집 (2015년 ~ 현재)
- 시간이 오래 걸리므로 백그라운드 실행 권장
- 진행상황 로그 파일 생성: collect_top200.log
"""

import psycopg2
import os
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime, timedelta
import time
import sys

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

# 로그 파일 설정
LOG_FILE = 'collect_top200.log'

def log(message):
    """화면과 파일에 동시에 로그 출력"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_message + '\n')

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_kospi_top200():
    """코스피 시가총액 상위 200개 종목 조회"""
    log("📊 코스피 시가총액 상위 200개 종목 조회 중...")

    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            df = stock.get_market_cap_by_ticker(date, market='KOSPI')
            if not df.empty:
                top200 = df.nlargest(200, '시가총액')
                if top200['시가총액'].sum() > 0:
                    log(f"✅ {date} 기준 코스피 상위 200개 종목 조회 완료")
                    return top200, date
        except Exception as e:
            if i < 5:
                log(f"  ⚠️  {date} 데이터 조회 실패: {e}")
            continue

    return None, None

def get_kosdaq_top200():
    """코스닥 시가총액 상위 200개 종목 조회"""
    log("📊 코스닥 시가총액 상위 200개 종목 조회 중...")

    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            df = stock.get_market_cap_by_ticker(date, market='KOSDAQ')
            if not df.empty:
                top200 = df.nlargest(200, '시가총액')
                if top200['시가총액'].sum() > 0:
                    log(f"✅ {date} 기준 코스닥 상위 200개 종목 조회 완료")
                    return top200, date
        except Exception as e:
            if i < 5:
                log(f"  ⚠️  {date} 데이터 조회 실패: {e}")
            continue

    return None, None

def get_etf_top200():
    """ETF 거래대금 상위 200개 종목 조회"""
    log("📊 ETF 거래대금 상위 종목 조회 중...")

    # 주요 ETF 200개 (실제로는 전체 ETF 리스트 중 거래량 상위)
    # 여기서는 알려진 주요 ETF들을 포함한 확장 리스트 사용
    major_etfs = [
        '069500', '102110', '114800', '122630', '251340', '229200', '233740',
        '278530', '252670', '371460', '364690', '360750', '143850', '148020',
        '232080', '069660', '091160', '091180', '091170', '168490',
        '091230', '091220', '091210', '091180', '091170', '091160', '091150',
        '091140', '091130', '091120', '091110', '091090', '091080', '091070',
        '091060', '091050', '091040', '091030', '091020', '091010', '090990',
        '102780', '108450', '108590', '114100', '117460', '117700', '122090',
        '130680', '130730', '138230', '138520', '138540', '138910', '138930',
        '139220', '139240', '139260', '139270', '139280', '139290', '140700',
        '140710', '140950', '152100', '152280', '152380', '152500', '152870',
        '153130', '157450', '157490', '158490', '167860', '182480', '182490',
        '183700', '183710', '184800', '185680', '189400', '190680', '195920',
        '195930', '195970', '195980', '196030', '199820', '200030', '203780',
        '204210', '204480', '204490', '205720', '207940', '208370', '214980',
        '217770', '217780', '218150', '219390', '219900', '220260', '221800',
        '222170', '222190', '222200', '225050', '225060', '225130', '226490',
        '227540', '227550', '228790', '228800', '228810', '229200', '232080',
        '232140', '233160', '233740', '234080', '238670', '238720', '238890',
        '241180', '241390', '243880', '244580', '244620', '244660', '245340',
        '245350', '245360', '245710', '247660', '250780', '251340', '251350',
        '252410', '252650', '252660', '252670', '253150', '253160', '253250',
        '261920', '261930', '261940', '261950', '261960', '261970', '261980',
        '261990', '262000', '267490', '267500', '268280', '269530', '270810',
        '271050', '271060', '272210', '272560', '272580', '273130', '273140',
        '278420', '278530', '280920', '280930', '282000', '282690', '282720',
        '284420', '284980', '285000', '285130', '287170', '287180', '287310',
        '287320', '287330', '287340', '287980', '288260', '290080', '290130',
        '290140', '292150', '292190', '292340', '293160', '293180', '293560',
    ]

    for i in range(30):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        try:
            etf_data = []
            log(f"  📅 {date} 데이터 조회 중...")

            for etf_code in major_etfs:
                try:
                    df = stock.get_etf_ohlcv_by_date(date, date, etf_code)

                    if not df.empty:
                        row = df.iloc[0]
                        etf_name = stock.get_etf_ticker_name(etf_code)
                        trading_value = int(row['종가'] * row['거래량'])

                        if trading_value > 0:
                            etf_data.append({
                                '종목코드': etf_code,
                                '종목명': etf_name,
                                '시가총액': trading_value,
                                '종가': int(row['종가']),
                                '거래량': int(row['거래량'])
                            })
                except:
                    continue

            if len(etf_data) >= 50:  # 최소 50개 이상 확보
                import pandas as pd
                df_result = pd.DataFrame(etf_data)
                top200 = df_result.nlargest(min(200, len(etf_data)), '시가총액')
                log(f"✅ {date} 기준 ETF 상위 {len(top200)}개 조회 완료")
                return top200, date
        except Exception as e:
            if i < 5:
                log(f"  ⚠️  {date} 데이터 조회 실패: {e}")
            continue

    return None, None

def insert_stock(stock_code, stock_name, market_cap, market_type):
    """종목 정보 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_cap, market_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_code)
            DO UPDATE SET
                market_cap = EXCLUDED.market_cap,
                market_type = EXCLUDED.market_type,
                stock_name = EXCLUDED.stock_name
        """, (stock_code, stock_name, market_cap, market_type))

        conn.commit()
    finally:
        cur.close()
        conn.close()

def collect_stock_historical_data(stock_code, start_date, end_date, market_type):
    """종목별 과거 데이터 수집 (일반 주식)"""
    try:
        df = stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)

        if df.empty:
            return 0

        conn = get_db_connection()
        cur = conn.cursor()

        count = 0
        for date, row in df.iterrows():
            trade_date = date.strftime('%Y-%m-%d')

            cur.execute("""
                INSERT INTO daily_prices
                (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code, trade_date) DO NOTHING
            """, (
                stock_code,
                trade_date,
                int(row['시가']),
                int(row['고가']),
                int(row['저가']),
                int(row['종가']),
                int(row['거래량'])
            ))
            count += 1

        conn.commit()
        cur.close()
        conn.close()

        return count

    except Exception as e:
        log(f"    ❌ 데이터 수집 실패: {e}")
        return 0

def collect_etf_historical_data(etf_code, start_date, end_date):
    """ETF별 과거 데이터 수집"""
    try:
        df = stock.get_etf_ohlcv_by_date(start_date, end_date, etf_code)

        if df.empty:
            return 0

        conn = get_db_connection()
        cur = conn.cursor()

        count = 0
        for date, row in df.iterrows():
            trade_date = date.strftime('%Y-%m-%d')

            cur.execute("""
                INSERT INTO daily_prices
                (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code, trade_date) DO NOTHING
            """, (
                etf_code,
                trade_date,
                int(row['시가']),
                int(row['고가']),
                int(row['저가']),
                int(row['종가']),
                int(row['거래량'])
            ))
            count += 1

        conn.commit()
        cur.close()
        conn.close()

        return count

    except Exception as e:
        log(f"    ❌ 데이터 수집 실패: {e}")
        return 0

def main():
    log("="*80)
    log("🚀 코스피/코스닥/ETF 상위 200개 종목 데이터 수집 (2015년~현재)")
    log("="*80)
    log(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"로그 파일: {LOG_FILE}\n")

    # 데이터 수집 기간 설정
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')
    log(f"📅 수집 기간: {start_date} ~ {end_date} (약 10년)\n")

    total_stocks = 0
    total_records = 0

    # 1. 코스피 상위 200개
    log("\n" + "="*80)
    log("1️⃣  코스피 상위 200개 종목 처리 시작")
    log("="*80)

    kospi_top200, ref_date = get_kospi_top200()

    if kospi_top200 is not None:
        log(f"📋 수집 대상: {len(kospi_top200)}개 종목\n")

        for idx, stock_code in enumerate(kospi_top200.index, 1):
            row = kospi_top200.loc[stock_code]
            stock_name = stock.get_market_ticker_name(stock_code)
            market_cap = int(row['시가총액'])

            log(f"[코스피 {idx}/{len(kospi_top200)}] {stock_name} ({stock_code})")
            log(f"  시가총액: {market_cap:,}원 ({market_cap/1_000_000_000_000:.2f}조)")

            # 종목 정보 저장
            insert_stock(stock_code, stock_name, market_cap, 'KOSPI')
            log(f"  ✅ 종목 정보 저장 완료")

            # 과거 데이터 수집 (2015년부터)
            log(f"  📈 2015년~현재 데이터 수집 중...")
            count = collect_stock_historical_data(stock_code, start_date, end_date, 'KOSPI')
            log(f"  ✅ {count}건 데이터 수집 완료\n")

            total_stocks += 1
            total_records += count

            time.sleep(0.5)  # API 과부하 방지
    else:
        log("❌ 코스피 데이터 조회 실패\n")

    # 2. 코스닥 상위 200개
    log("\n" + "="*80)
    log("2️⃣  코스닥 상위 200개 종목 처리 시작")
    log("="*80)

    kosdaq_top200, ref_date = get_kosdaq_top200()

    if kosdaq_top200 is not None:
        log(f"📋 수집 대상: {len(kosdaq_top200)}개 종목\n")

        for idx, stock_code in enumerate(kosdaq_top200.index, 1):
            row = kosdaq_top200.loc[stock_code]
            stock_name = stock.get_market_ticker_name(stock_code)
            market_cap = int(row['시가총액'])

            log(f"[코스닥 {idx}/{len(kosdaq_top200)}] {stock_name} ({stock_code})")
            log(f"  시가총액: {market_cap:,}원 ({market_cap/1_000_000_000_000:.2f}조)")

            # 종목 정보 저장
            insert_stock(stock_code, stock_name, market_cap, 'KOSDAQ')
            log(f"  ✅ 종목 정보 저장 완료")

            # 과거 데이터 수집 (2015년부터)
            log(f"  📈 2015년~현재 데이터 수집 중...")
            count = collect_stock_historical_data(stock_code, start_date, end_date, 'KOSDAQ')
            log(f"  ✅ {count}건 데이터 수집 완료\n")

            total_stocks += 1
            total_records += count

            time.sleep(0.5)  # API 과부하 방지
    else:
        log("❌ 코스닥 데이터 조회 실패\n")

    # 3. ETF 상위 200개
    log("\n" + "="*80)
    log("3️⃣  ETF 상위 종목 처리 시작")
    log("="*80)

    etf_top200, ref_date = get_etf_top200()

    if etf_top200 is not None:
        log(f"📋 수집 대상: {len(etf_top200)}개 종목\n")

        for idx, (_, row) in enumerate(etf_top200.iterrows(), 1):
            etf_code = row['종목코드']
            etf_name = row['종목명']
            trading_value = row['시가총액']

            log(f"[ETF {idx}/{len(etf_top200)}] {etf_name} ({etf_code})")
            log(f"  거래대금: {trading_value:,}원 ({trading_value/100_000_000:.0f}억)")

            # 종목 정보 저장
            insert_stock(etf_code, etf_name, trading_value, 'ETF')
            log(f"  ✅ 종목 정보 저장 완료")

            # 과거 데이터 수집 (2015년부터)
            log(f"  📈 2015년~현재 데이터 수집 중...")
            count = collect_etf_historical_data(etf_code, start_date, end_date)
            log(f"  ✅ {count}건 데이터 수집 완료\n")

            total_stocks += 1
            total_records += count

            time.sleep(0.5)  # API 과부하 방지
    else:
        log("❌ ETF 데이터 조회 실패\n")

    # 최종 요약
    log("\n" + "="*80)
    log("✅ 전체 데이터 수집 완료")
    log("="*80)
    log(f"총 종목 수: {total_stocks}개")
    log(f"총 레코드 수: {total_records:,}건")
    log(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("="*80)

if __name__ == "__main__":
    main()
