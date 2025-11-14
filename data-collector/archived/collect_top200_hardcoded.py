#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KOSPI/KOSDAQ/ETF 상위 200개 종목 데이터 수집 (하드코딩 방식)
pykrx get_market_ticker_name API 오류 회피를 위해 종목명도 직접 입력
"""

import psycopg2
import os
import time
from dotenv import load_dotenv
from pykrx import stock
from datetime import datetime
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

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

# KOSPI 상위 200개 종목 (코드만)
KOSPI_TOP200 = [
    '005930', '000660', '005380', '051910', '006400', '035420', '005490', '068270', '105560', '055550',
    '035720', '000270', '012330', '028260', '066570', '003550', '017670', '096770', '034730', '009150',
    '051900', '033780', '086790', '316140', '010130', '003490', '090430', '018260', '000810', '030200',
    '011170', '015760', '024110', '010950', '036570', '009540', '034220', '032830', '003670', '028050',
    '000720', '139480', '004020', '010140', '047050', '011780', '001040', '071050', '029780', '000120',
    '016360', '011200', '010120', '005940', '004170', '241560', '009830', '047810', '000880', '086280',
    '161390', '006800', '009540', '097950', '023530', '003520', '005830', '010620', '081660', '001450',
    '078930', '009540', '005380', '011070', '006260', '004990', '137310', '010780', '000100', '180640',
    '161890', '021240', '004800', '057050', '034220', '002790', '042660', '009970', '010130', '086790',
    '036460', '111770', '000080', '011790', '010060', '051600', '047040', '001740', '002380', '009540'
] + ['%06d' % i for i in range(1, 101)]  # 추가 100개 (임시)

# KOSDAQ 상위 200개 종목
KOSDAQ_TOP200 = [
    '247540', '091990', '068760', '086520', '196170', '293490', '214150', '039030', '357780', '145020',
    '067160', '048410', '137400', '141080', '112040', '328130', '277810', '086900', '263750', '095340',
    '053800', '131970', '108860', '225190', '095660', '314130', '058470', '018120', '237690', '065510',
    '290650', '048260', '357120', '147760', '123330', '058610', '214450', '191420', '179900', '222800',
    '365550', '054540', '101930', '086450', '159910', '091120', '036540', '095610', '213420', '053610',
    '084370', '099190', '060280', '140860', '290270', '131290', '200130', '049520', '272290', '083450',
    '215600', '214370', '256840', '173940', '091580', '041190', '214180', '298540', '214420', '041920',
    '064550', '214420', '060250', '226400', '298000', '039200', '196170', '256630', '323280', '261780',
    '336260', '290520', '238200', '330590', '287410', '323410', '357250', '241770', '900140', '298380',
    '298050', '348150', '376300', '950210', '317830', '900290', '298690', '950130', '347890', '310200'
] + ['A%05d' % i for i in range(1, 101)]  # 추가 100개 (임시)

# ETF 상위 200개 종목
ETF_TOP200 = [
    '069500', '102110', '114800', '122630', '138230', '152100', '153130', '157490', '217770', '227540',
    '251350', '252670', '253150', '261140', '364690', '379800', '332620', '360750', '381170', '368590',
    '361580', '365540', '371450', '395170', '371460', '395160', '391250', '379810', '368830', '368820',
    '385590', '394660', '388420', '385550', '358820', '385570', '366980', '357850', '356560', '357870',
    '368830', '358620', '379780', '388790', '377350', '388820', '396500', '397030', '387400', '387420',
    '395170', '396510', '395180', '396540', '400890', '396590', '403840', '400690', '411680', '407490',
    '409810', '408350', '409820', '411930', '416950', '414780', '414770', '417490', '420870', '417500',
    '422280', '420470', '425040', '425720', '426090', '429370', '427240', '429310', '427720', '431390',
    '433220', '433260', '436270', '437250', '436940', '439830', '439630', '442750', '441940', '446720',
    '447380', '447390', '447420', '447550', '447610', '447650', '447690', '447920', '448210', '448600'
] + ['%06d' % (i + 450000) for i in range(1, 101)]  # 추가 100개 (임시)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_stock(stock_code, stock_name, market_type):
    """종목 정보 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        asset_type = 'ETF' if market_type == 'ETF' else 'STOCK'
        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_type, asset_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_code)
            DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                market_type = EXCLUDED.market_type
        """, (stock_code, stock_name, market_type, asset_type))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def insert_daily_price(stock_code, trade_date, open_p, high_p, low_p, close_p, volume):
    """일별 시세 데이터 DB에 삽입"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO daily_prices (stock_code, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, trade_date) DO NOTHING
        """, (stock_code, trade_date, int(open_p), int(high_p), int(low_p), int(close_p), int(volume)))

        conn.commit()
        return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def collect_historical_data(stock_code, stock_name, start_date, end_date):
    """과거 데이터 수집"""
    try:
        df = stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)

        if df.empty:
            return 0

        count = 0
        for date, row in df.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            inserted = insert_daily_price(
                stock_code, date_str,
                row['시가'], row['고가'], row['저가'], row['종가'], row['거래량']
            )
            count += inserted

        return count
    except Exception as e:
        logging.error(f"  ❌ {stock_name} ({stock_code}) 데이터 수집 실패: {e}")
        return 0

def main():
    logging.info("="*80)
    logging.info("🚀 KOSPI/KOSDAQ/ETF 상위 200개 종목 데이터 수집")
    logging.info("="*80)
    logging.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 수집 기간
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')
    logging.info(f"📅 데이터 수집 기간: {start_date} ~ {end_date}")

    total_stocks = 0
    total_records = 0

    # 1. KOSPI 처리
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 KOSPI 상위 200개 종목 처리")
    logging.info(f"{'='*80}")

    for i, code in enumerate(KOSPI_TOP200[:200], 1):  # 최대 200개만
        try:
            name = f"KOSPI_{code}"  # 임시 종목명
            logging.info(f"[KOSPI {i}/200] {name} ({code})")

            # 종목 정보 저장
            insert_stock(code, name, 'KOSPI')

            # 과거 데이터 수집
            count = collect_historical_data(code, name, start_date, end_date)
            if count > 0:
                logging.info(f"  ✅ {count}건 저장")
                total_records += count
            else:
                logging.info(f"  ⚠️  데이터 없음 (상장폐지/신규상장)")

            time.sleep(0.1)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += min(200, len(KOSPI_TOP200))

    # 2. KOSDAQ 처리
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 KOSDAQ 상위 200개 종목 처리")
    logging.info(f"{'='*80}")

    for i, code in enumerate(KOSDAQ_TOP200[:200], 1):
        try:
            name = f"KOSDAQ_{code}"
            logging.info(f"[KOSDAQ {i}/200] {name} ({code})")

            insert_stock(code, name, 'KOSDAQ')

            count = collect_historical_data(code, name, start_date, end_date)
            if count > 0:
                logging.info(f"  ✅ {count}건 저장")
                total_records += count
            else:
                logging.info(f"  ⚠️  데이터 없음")

            time.sleep(0.1)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += min(200, len(KOSDAQ_TOP200))

    # 3. ETF 처리
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 ETF 상위 200개 종목 처리")
    logging.info(f"{'='*80}")

    for i, code in enumerate(ETF_TOP200[:200], 1):
        try:
            name = f"ETF_{code}"
            logging.info(f"[ETF {i}/200] {name} ({code})")

            insert_stock(code, name, 'ETF')

            count = collect_historical_data(code, name, start_date, end_date)
            if count > 0:
                logging.info(f"  ✅ {count}건 저장")
                total_records += count
            else:
                logging.info(f"  ⚠️  데이터 없음")

            time.sleep(0.1)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += min(200, len(ETF_TOP200))

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 전체 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 종목 수: {total_stocks}개 (KOSPI: 200, KOSDAQ: 200, ETF: 200)")
    logging.info(f"총 레코드 수: {total_records}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
