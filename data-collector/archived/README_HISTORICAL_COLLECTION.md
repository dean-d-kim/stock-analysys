# 전체 히스토리 데이터 수집 가이드

## 개요

공공데이터포털 API를 사용하여 1990년부터 현재까지의 모든 주식 및 ETF 데이터를 수집합니다.

## 수집 스크립트

### 1. collect_full_historical.py
전체 기간 (1990-01-01 ~ 현재)의 데이터를 수집하는 메인 스크립트

**특징:**
- 1990년 1월 1일부터 현재까지 모든 거래일 데이터 수집
- 주식(KOSPI/KOSDAQ)과 ETF 데이터 모두 수집
- 주말 자동 스킵
- 진행률 실시간 표시
- API 제한을 고려한 안전한 속도 제어
- 자동 재시도 및 오류 처리

**수집 데이터:**

주식 (stocks 테이블):
- stock_code: 종목코드
- stock_name: 종목명
- market_type: 시장구분 (KOSPI/KOSDAQ/ETF)
- isin_code: ISIN 코드
- listed_shares: 상장주식수
- market_cap: 시가총액
- nav: NAV (ETF 전용)
- net_asset_total: 순자산총액 (ETF 전용)
- base_index_name: 기초지수명 (ETF 전용)
- base_index_close: 기초지수종가 (ETF 전용)

일별 시세 (daily_prices 테이블):
- stock_code: 종목코드
- trade_date: 거래일
- open_price: 시가
- high_price: 고가
- low_price: 저가
- close_price: 종가
- volume: 거래량
- vs: 전일대비
- change_rate: 등락율
- trading_value: 거래대금

### 2. test_historical.py
소규모 테스트용 스크립트 (최근 1주일 데이터만 수집)

## 사용 방법

### 전체 히스토리 수집 실행

```bash
cd /Users/ddkim/stock-analysis-project/data-collector

# 백그라운드 실행 (권장)
python3 -u collect_full_historical.py 2>&1 | tee collect_full_historical.log &

# 또는 포그라운드 실행
python3 collect_full_historical.py
```

### 테스트 실행 (1주일)

```bash
python3 test_historical.py
```

### 진행 상황 확인

```bash
# 로그 파일 실시간 확인
tail -f collect_full_historical.log

# 최근 100줄 확인
tail -100 collect_full_historical.log

# 진행률 확인 (grep으로 필터링)
grep "진행률" collect_full_historical.log | tail -5
```

### 데이터 확인

```bash
# DB 상태 확인
python3 check_db_status.py

# 새로운 필드 검증
python3 verify_new_fields.py
```

## 예상 소요 시간

- 총 처리 대상: 약 13,096일 (1990-01-01 ~ 현재)
- 주말 제외: 약 9,370일 (영업일만)
- 휴장일 고려: 약 6,000~7,000일 (실제 거래일)

각 날짜당 처리 시간: 약 1~2초
- 전체 예상 소요 시간: **3~4시간** (API 속도에 따라 변동)

## 주의사항

1. **API 키 확인**
   - `.env` 파일에 `DATA_GO_KR_API_KEY`가 설정되어 있어야 합니다
   - 공공데이터포털에서 발급받은 디코딩 키를 사용하세요

2. **데이터베이스 공간**
   - 35년치 데이터로 상당한 저장 공간이 필요합니다
   - 예상 데이터 크기: 종목수 x 거래일수 x 레코드 크기
   - KOSPI/KOSDAQ 합계 약 2,500개 종목
   - ETF 약 600개
   - 총 레코드 수: 수백만~천만 건 예상

3. **API 제한**
   - 공공데이터포털 API 일일 호출 한도 확인 필요
   - 스크립트는 1초당 1~2회 정도로 호출 제한

4. **네트워크 안정성**
   - 장시간 실행되므로 안정적인 네트워크 환경 필요
   - 중단 시 재시작하면 UPSERT로 중복 없이 이어서 수집 가능

5. **ETF 데이터**
   - ETF는 2000년대 중반부터 시작
   - 1990년대 데이터에서 404 오류는 정상 (ETF 없음)

## 트러블슈팅

### 문제: API 응답이 느림
```bash
# 네트워크 상태 확인
ping apis.data.go.kr

# API 키 테스트
python3 test_data_go_kr.py
```

### 문제: 데이터베이스 연결 오류
```bash
# DB 연결 테스트
python3 -c "from collect_full_historical import get_db_connection; conn = get_db_connection(); print('연결 성공'); conn.close()"
```

### 문제: 중간에 중단됨
- 걱정하지 마세요! 스크립트를 다시 실행하면 됩니다
- `ON CONFLICT DO UPDATE` 구문으로 중복 데이터 자동 처리
- 이미 수집된 데이터는 업데이트되고, 없는 데이터만 추가됩니다

## 수집 완료 후

### 1. 데이터 검증
```bash
python3 check_db_status.py
python3 verify_new_fields.py
```

### 2. 통계 확인
```sql
-- 거래일별 레코드 수
SELECT trade_date, COUNT(*)
FROM daily_prices
GROUP BY trade_date
ORDER BY trade_date DESC
LIMIT 10;

-- 연도별 총 레코드 수
SELECT EXTRACT(YEAR FROM trade_date) as year, COUNT(*)
FROM daily_prices
GROUP BY year
ORDER BY year;

-- 시장별 종목 수
SELECT market_type, COUNT(*)
FROM stocks
GROUP BY market_type;
```

### 3. 로그 분석
```bash
# 성공률 확인
grep "성공한 날" collect_full_historical.log | tail -1

# 오류 확인
grep "❌" collect_full_historical.log | wc -l

# 최종 요약
tail -20 collect_full_historical.log
```

## 향후 업데이트

일일 업데이트를 위해서는 다음 스크립트를 사용하세요:
```bash
# 최근 7일 데이터 업데이트
python3 collect_data_go_kr.py

# ETF 데이터 업데이트
python3 collect_etf_go_kr.py
```

## 백업 권장사항

대량 데이터 수집 전후로 데이터베이스 백업을 권장합니다:
```bash
# PostgreSQL 백업
pg_dump -h 124.54.191.68 -p 5433 -U stock_user -d stock_analysis > backup_before_historical.sql

# 복원 (필요시)
psql -h 124.54.191.68 -p 5433 -U stock_user -d stock_analysis < backup_before_historical.sql
```
