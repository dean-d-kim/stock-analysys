#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
네이버 금융에서 PER, PBR 조회 및 DB 업데이트
DART에서 EPS, BPS 및 공시일 조회 (선택적)
"""

import psycopg2
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import time
from pykrx import stock
from bs4 import BeautifulSoup

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '124.54.191.68'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'stock_user'),
    'password': os.getenv('DB_PASSWORD', 'StockDB2025!')
}

DART_API_KEY = os.getenv('DART_API_KEY', '')

# DART 기업 고유번호 매핑
CORP_CODE_MAPPING = {
    '005930': '00126380',  # 삼성전자
    '000660': '00164779',  # SK하이닉스
    '035420': '00414721',  # NAVER
    '051910': '00406951',  # LG화학
    '005380': '00119059',  # 현대차
    '006400': '00164742',  # 삼성SDI
    '000270': '00162343',  # 기아
    '035720': '00413273',  # 카카오
    '028260': '00164529',  # 삼성물산
    '012330': '00164779',  # 현대모비스
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def add_columns_if_not_exist():
    """필요한 컬럼 추가"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        print("📊 필요한 컬럼 추가 중...")

        # PER 컬럼
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='per'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE stocks ADD COLUMN per DECIMAL(10,2)")
            print("  ✅ PER 컬럼 추가")

        # PBR 컬럼
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='pbr'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE stocks ADD COLUMN pbr DECIMAL(10,2)")
            print("  ✅ PBR 컬럼 추가")

        # 공시일 컬럼
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='last_report_date'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE stocks ADD COLUMN last_report_date VARCHAR(20)")
            print("  ✅ last_report_date 컬럼 추가")

        # EPS (주당순이익) 컬럼
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='eps'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE stocks ADD COLUMN eps INTEGER")
            print("  ✅ EPS 컬럼 추가")

        # BPS (주당순자산) 컬럼
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='stocks' AND column_name='bps'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE stocks ADD COLUMN bps INTEGER")
            print("  ✅ BPS 컬럼 추가")

        conn.commit()
        print("✅ 컬럼 추가 완료\n")

    except Exception as e:
        conn.rollback()
        print(f"❌ 컬럼 추가 실패: {e}")
    finally:
        cur.close()
        conn.close()

def get_latest_financial_data(corp_code, stock_code, stock_name):
    """DART에서 최근 4개 분기 재무 데이터 조회 (TTM)"""

    if not DART_API_KEY:
        return None

    # 최신 분기 찾기
    current_year = datetime.now().year
    latest_quarter_data = None

    for year in [current_year, current_year - 1]:
        for quarter in [3, 2, 1]:
            reprt_code = f'110{quarter:02d}'

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

                if data.get('status') == '000' and 'list' in data:
                    latest_quarter_data = {
                        'year': year,
                        'quarter': quarter,
                        'data': data['list']
                    }
                    break

                time.sleep(0.1)

            except Exception as e:
                continue

        if latest_quarter_data:
            break

    if not latest_quarter_data:
        return None

    # 최근 4개 분기 데이터 수집
    quarters_data = [latest_quarter_data]
    base_year = latest_quarter_data['year']
    base_quarter = latest_quarter_data['quarter']

    # 나머지 3개 분기 조회
    for i in range(1, 4):
        q = base_quarter - i
        y = base_year

        if q <= 0:
            q += 4
            y -= 1

        reprt_code = f'110{q:02d}' if q <= 3 else '11011'  # 4분기는 사업보고서

        params = {
            'crtfc_key': DART_API_KEY,
            'corp_code': corp_code,
            'bsns_year': y,
            'reprt_code': reprt_code
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if data.get('status') == '000' and 'list' in data:
                quarters_data.append({
                    'year': y,
                    'quarter': q,
                    'data': data['list']
                })

            time.sleep(0.1)

        except Exception as e:
            continue

    report_date = f"{base_year}년 {base_quarter}분기 (TTM)"

    return {
        'quarters_data': quarters_data,
        'year': base_year,
        'quarter': base_quarter,
        'report_date': report_date
    }

def extract_financial_metrics(quarters_data):
    """TTM 방식으로 재무 데이터 추출 - 최근 4개 분기 합산"""

    # 최근 4개 분기의 당기순이익 합산
    total_net_income = 0
    net_income_count = 0

    # 최신 분기의 자본총계 사용
    equity = None

    for quarter_info in quarters_data:
        financial_data = quarter_info['data']

        for item in financial_data:
            account_nm = item.get('account_nm', '')
            thstrm_amount = item.get('thstrm_amount', '0')

            # 당기순이익 합산 (4개 분기)
            if '당기순이익' in account_nm and net_income_count < 4:
                try:
                    amount = int(thstrm_amount.replace(',', ''))
                    total_net_income += amount
                    net_income_count += 1
                    break  # 해당 분기의 순이익을 찾았으므로 다음 분기로
                except:
                    pass

    # 최신 분기의 자본총계
    latest_data = quarters_data[0]['data']
    for item in latest_data:
        account_nm = item.get('account_nm', '')
        thstrm_amount = item.get('thstrm_amount', '0')

        if account_nm == '자본총계':
            try:
                equity = int(thstrm_amount.replace(',', ''))
                break
            except:
                pass

    return {
        '당기순이익': total_net_income if net_income_count > 0 else None,
        '자본총계': equity
    }

def calculate_per_pbr(stock_code, metrics, current_price, shares_outstanding):
    """PER, PBR 계산"""

    result = {
        'per': None,
        'pbr': None,
        'eps': None,
        'bps': None
    }

    # 상장주식수 확인
    if not shares_outstanding or shares_outstanding <= 0:
        return result

    # EPS 계산 (당기순이익 / 발행주식수)
    if metrics['당기순이익'] and shares_outstanding > 0:
        # DART 데이터는 이미 원 단위이므로 변환하지 않음
        net_income = metrics['당기순이익']
        eps = net_income / shares_outstanding
        result['eps'] = int(eps)

        # PER 계산 (현재가 / EPS)
        if eps > 0 and current_price:
            per = current_price / eps
            result['per'] = round(per, 2)

    # BPS 계산 (자본총계 / 발행주식수)
    if metrics['자본총계'] and shares_outstanding > 0:
        # DART 데이터는 이미 원 단위이므로 변환하지 않음
        equity = metrics['자본총계']
        bps = equity / shares_outstanding
        result['bps'] = int(bps)

        # PBR 계산 (현재가 / BPS)
        if bps > 0 and current_price:
            pbr = current_price / bps
            result['pbr'] = round(pbr, 2)

    return result

def get_current_price(stock_code):
    """DB에서 최근 종가 조회"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT close_price
            FROM daily_prices
            WHERE stock_code = %s
            ORDER BY trade_date DESC
            LIMIT 1
        """, (stock_code,))

        result = cur.fetchone()
        return result[0] if result else None

    finally:
        cur.close()
        conn.close()

def get_shares_outstanding(stock_code):
    """pykrx에서 상장주식수 조회"""
    # 최근 거래일 찾기 (최대 15일 전까지)
    for i in range(15):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')

        # KOSPI와 KOSDAQ 모두 시도
        for market in ['KOSPI', 'KOSDAQ']:
            try:
                df = stock.get_market_cap_by_ticker(date, market=market)
                if not df.empty and stock_code in df.index:
                    row = df.loc[stock_code]
                    # 상장주식수 컬럼이 있는지 확인
                    if isinstance(row, dict) or hasattr(row, 'index'):
                        if isinstance(row, dict):
                            shares = row.get('상장주식수', 0)
                        else:
                            shares = row.get('상장주식수', 0) if '상장주식수' in row.index else 0

                        if shares > 0:
                            return int(shares)
            except Exception as e:
                # 에러는 무시하고 계속
                pass
    return None

def get_per_pbr_from_naver(stock_code):
    """네이버 금융에서 PER, PBR 직접 조회"""

    url = f'https://finance.naver.com/item/main.naver?code={stock_code}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # PER 추출 (em#_per)
        per_element = soup.select_one('em#_per')
        per = None
        if per_element and per_element.text.strip():
            try:
                per_text = per_element.text.strip().replace(',', '')
                per = float(per_text)
            except ValueError:
                pass

        # PBR 추출 (em#_pbr)
        pbr_element = soup.select_one('em#_pbr')
        pbr = None
        if pbr_element and pbr_element.text.strip():
            try:
                pbr_text = pbr_element.text.strip().replace(',', '')
                pbr = float(pbr_text)
            except ValueError:
                pass

        return {
            'per': per,
            'pbr': pbr
        }

    except Exception as e:
        print(f"  ⚠️  네이버 금융 조회 실패: {e}")
        return {
            'per': None,
            'pbr': None
        }

def update_stock_metrics(stock_code, per, pbr, eps, bps, report_date):
    """stocks 테이블 업데이트"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # numpy 타입을 Python 기본 타입으로 변환
        per_val = float(per) if per is not None else None
        pbr_val = float(pbr) if pbr is not None else None
        eps_val = int(eps) if eps is not None else None
        bps_val = int(bps) if bps is not None else None

        cur.execute("""
            UPDATE stocks
            SET per = %s, pbr = %s, eps = %s, bps = %s, last_report_date = %s
            WHERE stock_code = %s
        """, (per_val, pbr_val, eps_val, bps_val, report_date, stock_code))

        conn.commit()

    finally:
        cur.close()
        conn.close()

def main():
    print("="*80)
    print("🚀 네이버 금융 기반 PER/PBR 업데이트")
    print("="*80)
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. 필요한 컬럼 추가
    add_columns_if_not_exist()

    # 2. DB에서 종목 목록 조회
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT stock_code, stock_name, market_cap FROM stocks ORDER BY market_cap DESC NULLS LAST")
    stocks = cur.fetchall()
    cur.close()
    conn.close()

    print(f"📊 총 {len(stocks)}개 종목 처리 시작\n")

    success_count = 0
    skip_count = 0

    for idx, (stock_code, stock_name, market_cap) in enumerate(stocks, 1):
        print(f"[{idx}/{len(stocks)}] {stock_name} ({stock_code})")

        # 1. 네이버 금융에서 PER/PBR 조회 (모든 종목)
        print(f"  🔍 네이버 금융에서 PER/PBR 조회 중...")
        naver_data = get_per_pbr_from_naver(stock_code)

        per_from_naver = naver_data['per']
        pbr_from_naver = naver_data['pbr']

        if per_from_naver or pbr_from_naver:
            per_str = f"{per_from_naver:.2f}" if per_from_naver else "N/A"
            pbr_str = f"{pbr_from_naver:.2f}" if pbr_from_naver else "N/A"

            print(f"  ✅ 네이버 PER: {per_str}, PBR: {pbr_str}")
        else:
            print(f"  ⚠️  네이버에서 PER/PBR 조회 실패")

        # 2. DART에서 EPS/BPS 및 공시일 조회 (선택적)
        eps_val = None
        bps_val = None
        report_date = None

        corp_code = CORP_CODE_MAPPING.get(stock_code)
        if corp_code:
            print(f"  🔍 DART 공시 정보 조회 중...")
            financial_info = get_latest_financial_data(corp_code, stock_code, stock_name)

            if financial_info:
                print(f"  ✅ {financial_info['report_date']} 데이터 발견")
                report_date = financial_info['report_date']

                # 재무 지표 추출 (TTM)
                metrics = extract_financial_metrics(financial_info['quarters_data'])

                # 현재가 및 상장주식수 조회
                current_price = get_current_price(stock_code)
                shares_outstanding = get_shares_outstanding(stock_code)

                if current_price and shares_outstanding:
                    # EPS, BPS만 계산 (참고용)
                    dart_result = calculate_per_pbr(stock_code, metrics, current_price, shares_outstanding)
                    eps_val = dart_result['eps']
                    bps_val = dart_result['bps']

                    eps_str = f"{eps_val:,}원" if eps_val else "N/A"
                    bps_str = f"{bps_val:,}원" if bps_val else "N/A"
                    print(f"  📈 DART EPS: {eps_str}, BPS: {bps_str}")

        # 3. DB 업데이트 (네이버 PER/PBR + DART EPS/BPS)
        if per_from_naver or pbr_from_naver:
            update_stock_metrics(
                stock_code,
                per_from_naver,
                pbr_from_naver,
                eps_val,
                bps_val,
                report_date
            )

            print(f"  ✅ DB 업데이트 완료\n")
            success_count += 1
        else:
            print(f"  ⚠️  PER/PBR 정보 없음 - 건너뜀\n")
            skip_count += 1

        time.sleep(0.5)  # 크롤링 과부하 방지

    print("="*80)
    print(f"✅ 처리 완료")
    print(f"   성공: {success_count}개")
    print(f"   건너뜀: {skip_count}개")
    print("="*80)

    # 결과 요약
    print_summary()

def print_summary():
    """결과 요약 출력"""
    conn = get_db_connection()
    cur = conn.cursor()

    print("\n" + "="*80)
    print("📊 PER/PBR 업데이트 결과 (상위 10개)")
    print("="*80)

    cur.execute("""
        SELECT stock_code, stock_name, per, pbr, eps, bps, last_report_date
        FROM stocks
        WHERE per IS NOT NULL OR pbr IS NOT NULL
        ORDER BY market_cap DESC NULLS LAST
        LIMIT 10
    """)

    print(f"{'순위':<4} {'종목명':<15} {'PER':<8} {'PBR':<8} {'EPS':<12} {'BPS':<12} {'공시일':<15}")
    print("-"*80)

    for idx, (code, name, per, pbr, eps, bps, report_date) in enumerate(cur.fetchall(), 1):
        per_str = f"{per:.2f}" if per else "N/A"
        pbr_str = f"{pbr:.2f}" if pbr else "N/A"
        eps_str = f"{eps:,}원" if eps else "N/A"
        bps_str = f"{bps:,}원" if bps else "N/A"
        report_str = report_date if report_date else "N/A"

        print(f"{idx:<4} {name:<15} {per_str:<8} {pbr_str:<8} {eps_str:<12} {bps_str:<12} {report_str:<15}")

    print("="*80)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
