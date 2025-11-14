#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
한국 주요 종목 (KOSPI/KOSDAQ/ETF) 데이터 수집
API 호출 최소화를 위해 주요 종목 리스트 하드코딩
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

# KOSPI 주요 200개 종목 (시가총액 상위)
KOSPI_MAJOR = [
    '005930',  # 삼성전자
    '000660',  # SK하이닉스
    '005380',  # 현대차
    '051910',  # LG화학
    '006400',  # 삼성SDI
    '035420',  # NAVER
    '005490',  # POSCO홀딩스
    '068270',  # 셀트리온
    '105560',  # KB금융
    '055550',  # 신한지주
    '035720',  # 카카오
    '000270',  # 기아
    '012330',  # 현대모비스
    '028260',  # 삼성물산
    '066570',  # LG전자
    '003550',  # LG
    '017670',  # SK텔레콤
    '096770',  # SK이노베이션
    '034730',  # SK
    '009150',  # 삼성전기
    '051900',  # LG생활건강
    '033780',  # KT&G
    '086790',  # 하나금융지주
    '316140',  # 우리금융지주
    '010130',  # 고려아연
    '003490',  # 대한항공
    '090430',  # 아모레퍼시픽
    '018260',  # 삼성에스디에스
    '000810',  # 삼성화재
    '030200',  # KT
    '011170',  # 롯데케미칼
    '015760',  # 한국전력
    '024110',  # 기업은행
    '010950',  # S-Oil
    '036570',  # 엔씨소프트
    '009540',  # 한국조선해양
    '034220',  # LG디스플레이
    '032830',  # 삼성생명
    '003670',  # 포스코퓨처엠
    '028050',  # 삼성엔지니어링
    '000720',  # 현대건설
    '139480',  # 이마트
    '004020',  # 현대제철
    '010140',  # 삼성중공업
    '047050',  # 포스코인터내셔널
    '011780',  # 금호석유
    '001040',  # CJ
    '071050',  # 한국금융지주
    '029780',  # 삼성카드
    '000120',  # CJ대한통운
]

# KOSDAQ 주요 150개 종목 (시가총액 상위)
KOSDAQ_MAJOR = [
    '247540',  # 에코프로비엠
    '091990',  # 셀트리온헬스케어
    '068760',  # 셀트리온제약
    '086520',  # 에코프로
    '196170',  # 알테오젠
    '293490',  # 카카오게임즈
    '214150',  # 클래시스
    '039030',  # 이오테크닉스
    '357780',  # 솔브레인
    '145020',  # 휴젤
    '067160',  # 아프리카TV
    '048410',  # 현대바이오
    '137400',  # 피엔티
    '141080',  # 레고켐바이오
    '112040',  # 위메이드
    '328130',  # 루닛
    '277810',  # 레인보우로보틱스
    '086900',  # 메디톡스
    '263750',  # 펄어비스
    '095340',  # ISC
    '053800',  # 안랩
    '131970',  # 티웨이항공
    '108860',  # 셀바스AI
    '225190',  # 크로바하이텍
    '095660',  # 네오위즈
    '314130',  # 지놈앤컴퍼니
    '058470',  # 리노공업
    '018120',  # 진원생명과학
    '237690',  # 코리안리재보험
    '065510',  # 휴맥스
    '290650',  # 엘앤씨바이오
    '048260',  # 오스템임플란트
    '357120',  # 코람코에너지리츠
    '147760',  # 피엠그로우
    '123330',  # 제닉
    '058610',  # 포스코아이씨티
    '214450',  # 파마리서치
    '191420',  # 테고사이언스
    '179900',  # 유티아이
    '222800',  # 심텍
    '365550',  # ESR켄달스퀘어리츠
    '054540',  # 삼영전자
    '101930',  # 인바디
    '086450',  # 동국제약
    '159910',  # 스튜디오드래곤
]

# ETF 주요 50개
ETF_MAJOR = [
    '069500',  # KODEX 200
    '102110',  # TIGER 200
    '114800',  # KODEX 인버스
    '122630',  # KODEX 레버리지
    '138230',  # KODEX 코스닥150레버리지
    '152100',  # ARIRANG 200
    '153130',  # KODEX 단기채권
    '157490',  # TIGER 소프트웨어
    '217770',  # TIGER S&P500
    '227540',  # TIGER 200IT
    '251350',  # KODEX 코스닥150선물인버스
    '252670',  # KODEX 200선물인버스2X
    '253150',  # TIGER 차이나전기차SOLACTIVE
    '261140',  # KINDEX 미국S&P500
    '364690',  # KBSTAR 미국나스닥100
    '379800',  # KODEX 미국S&P500TR
    '332620',  # KODEX 2차전지산업
    '360750',  # TIGER 미국나스닥100
    '381170',  # TIGER 미국테크TOP10 INDXX
    '368590',  # TIGER 차이나항셍TECH
    '361580',  # KODEX 코스피고배당
    '365540',  # KODEX CD금리투자KIS
    '371450',  # KODEX 미국S&P바이오
]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_stock(stock_code, stock_name, market_type):
    """종목 정보 DB에 삽입 (시가총액은 나중에 계산)"""
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
        logging.error(f"  ❌ {stock_name} 데이터 수집 실패: {e}")
        return 0

def get_stock_name(stock_code):
    """종목명 조회"""
    try:
        return stock.get_market_ticker_name(stock_code)
    except:
        return f"종목_{stock_code}"

def main():
    logging.info("="*80)
    logging.info("🚀 한국 주요 종목 데이터 수집")
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
    logging.info(f"📊 KOSPI 주요 {len(KOSPI_MAJOR)}개 종목 처리")
    logging.info(f"{'='*80}")

    for i, code in enumerate(KOSPI_MAJOR, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[KOSPI {i}/{len(KOSPI_MAJOR)}] {name} ({code})")

            # 종목 정보 저장
            insert_stock(code, name, 'KOSPI')

            # 과거 데이터 수집
            count = collect_historical_data(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.2)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(KOSPI_MAJOR)

    # 2. KOSDAQ 처리
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 KOSDAQ 주요 {len(KOSDAQ_MAJOR)}개 종목 처리")
    logging.info(f"{'='*80}")

    for i, code in enumerate(KOSDAQ_MAJOR, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[KOSDAQ {i}/{len(KOSDAQ_MAJOR)}] {name} ({code})")

            insert_stock(code, name, 'KOSDAQ')

            count = collect_historical_data(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.2)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(KOSDAQ_MAJOR)

    # 3. ETF 처리
    logging.info(f"\n{'='*80}")
    logging.info(f"📊 ETF 주요 {len(ETF_MAJOR)}개 종목 처리")
    logging.info(f"{'='*80}")

    for i, code in enumerate(ETF_MAJOR, 1):
        try:
            name = get_stock_name(code)
            logging.info(f"[ETF {i}/{len(ETF_MAJOR)}] {name} ({code})")

            insert_stock(code, name, 'ETF')

            count = collect_historical_data(code, name, start_date, end_date)
            logging.info(f"  ✅ {count}건 저장")

            total_records += count
            time.sleep(0.2)
        except Exception as e:
            logging.error(f"  ❌ {code} 처리 실패: {e}")
            continue

    total_stocks += len(ETF_MAJOR)

    # 최종 요약
    logging.info(f"\n{'='*80}")
    logging.info("✅ 전체 데이터 수집 완료")
    logging.info(f"{'='*80}")
    logging.info(f"총 종목 수: {total_stocks}개 (KOSPI: {len(KOSPI_MAJOR)}, KOSDAQ: {len(KOSDAQ_MAJOR)}, ETF: {len(ETF_MAJOR)})")
    logging.info(f"총 레코드 수: {total_records}건")
    logging.info(f"완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main()
