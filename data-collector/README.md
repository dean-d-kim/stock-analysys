# Stock Data Collector

공공데이터포털 API와 DART API를 사용하여 한국 주식 시장 데이터를 수집하는 스크립트 모음입니다.

## 디렉토리 구조

```
data-collector/
├── collect_data_go_kr.py       # 일일 주식 데이터 수집 (메인)
├── collect_etf_go_kr.py         # 일일 ETF 데이터 수집
├── collect_full_historical.py  # 전체 히스토리 데이터 수집 (2020~현재)
├── check_db_status.py           # 데이터베이스 상태 확인
├── verify_new_fields.py         # 새로 추가된 필드 검증
├── dart_api.py                  # DART API 연동
├── check_dart_latest.py         # DART 최신 재무제표 확인
├── db_handler.py                # 데이터베이스 핸들러
├── tests/                       # 테스트 스크립트
│   ├── test_data_go_kr.py      # API 연결 테스트
│   ├── test_historical.py      # 히스토리 수집 테스트 (1주일)
│   └── find_data_start_year.py # API 데이터 제공 시작 연도 확인
└── archived/                    # 사용하지 않는 구버전 스크립트
```

## 주요 스크립트

### 1. 일일 데이터 수집

#### collect_data_go_kr.py
**용도**: 매일 주식 시장 데이터 자동 수집 (KOSPI/KOSDAQ)

**수집 데이터**:
- 종목 기본 정보 (종목코드, 종목명, 시장구분)
- 일별 시세 (OHLCV, 전일대비, 등락율, 거래대금)
- 추가 정보 (ISIN 코드, 상장주식수, 시가총액)

**실행 방법**:
```bash
python3 collect_data_go_kr.py
```

**크론탭 설정** (매일 오후 6시 자동 실행):
```bash
0 18 * * 1-5 cd /Users/ddkim/stock-analysis-project/data-collector && python3 collect_data_go_kr.py >> collect_data_go_kr.log 2>&1
```

#### collect_etf_go_kr.py
**용도**: 매일 ETF 데이터 자동 수집

**수집 데이터**:
- ETF 기본 정보
- 일별 시세 (OHLCV)
- ETF 전용 필드 (NAV, 순자산총액, 기초지수명, 기초지수종가)

**실행 방법**:
```bash
python3 collect_etf_go_kr.py
```

### 2. 히스토리 데이터 수집

#### collect_full_historical.py
**용도**: 공공데이터포털 API가 제공하는 전체 기간 데이터 수집 (2020년 1월 ~ 현재)

**특징**:
- 2020년 1월 1일부터 현재까지 모든 영업일 데이터 수집
- 주말 자동 스킵
- 주식 + ETF 데이터 모두 수집
- UPSERT 패턴으로 중복 없이 안전하게 저장
- 중단되어도 재시작 시 이어서 수집 가능

**실행 방법**:
```bash
# 백그라운드 실행 (권장)
python3 -u collect_full_historical.py 2>&1 | tee collect_full_historical.log &

# 포그라운드 실행
python3 collect_full_historical.py
```

**예상 소요 시간**: 약 12-15시간
- 총 영업일: 약 1,500일
- 날짜당 처리 시간: 약 25-30초

**진행 상황 모니터링**:
```bash
# 로그 실시간 확인
tail -f collect_full_historical.log

# 진행률 확인
grep "진행률" collect_full_historical.log | tail -5
```

### 3. 데이터 확인 및 검증

#### check_db_status.py
**용도**: 데이터베이스 상태 및 통계 확인

**출력 정보**:
- 총 종목 수 (KOSPI/KOSDAQ/ETF별)
- 일별 시세 레코드 수
- 시가총액 통계
- 최근 업데이트 시각
- 시가총액 상위 10개 종목

**실행 방법**:
```bash
python3 check_db_status.py
```

#### verify_new_fields.py
**용도**: 새로 추가된 필드가 제대로 저장되었는지 검증

**검증 항목**:
- ISIN 코드, 상장주식수, 시가총액 (주식)
- NAV, 순자산총액, 기초지수 (ETF)
- 전일대비, 등락율, 거래대금 (일별 시세)

**실행 방법**:
```bash
python3 verify_new_fields.py
```

### 4. DART API 연동

#### dart_api.py
**용도**: DART API 연동 라이브러리

**기능**:
- 재무제표 조회
- 기업 기본 정보 조회

#### check_dart_latest.py
**용도**: DART 최신 재무제표 확인

**실행 방법**:
```bash
python3 check_dart_latest.py
```

## 환경 설정

### 1. 환경 변수 (.env 파일)

```env
# 데이터베이스 설정
DB_HOST=124.54.191.68
DB_PORT=5433
DB_NAME=stock_analysis
DB_USER=stock_user
DB_PASSWORD=your_password_here

# API 키
DATA_GO_KR_API_KEY=your_api_key_here   # 공공데이터포털 API 키
DART_API_KEY=your_dart_key_here         # DART API 키
```

### 2. Python 패키지 설치

```bash
pip3 install -r requirements.txt
```

필요한 패키지:
- psycopg2-binary (PostgreSQL 연동)
- requests (API 호출)
- python-dotenv (환경 변수 관리)

## 데이터베이스 스키마

### stocks 테이블
```sql
- stock_code (PK)       # 종목코드
- stock_name            # 종목명
- market_type           # 시장구분 (KOSPI/KOSDAQ/ETF)
- asset_type            # 자산유형 (STOCK/ETF)
- isin_code             # ISIN 코드
- listed_shares         # 상장주식수
- market_cap            # 시가총액
- nav                   # NAV (ETF 전용)
- net_asset_total       # 순자산총액 (ETF 전용)
- base_index_name       # 기초지수명 (ETF 전용)
- base_index_close      # 기초지수종가 (ETF 전용)
```

### daily_prices 테이블
```sql
- stock_code (PK)       # 종목코드
- trade_date (PK)       # 거래일
- open_price            # 시가
- high_price            # 고가
- low_price             # 저가
- close_price           # 종가
- volume                # 거래량
- vs                    # 전일대비
- change_rate           # 등락율
- trading_value         # 거래대금
```

## API 정보

### 공공데이터포털 API
- **제공 기간**: 2020년 1월 ~ 현재
- **제공 데이터**: 주식 및 ETF 일별 시세
- **API 문서**: https://www.data.go.kr/
- **호출 제한**: 일일 호출 한도 확인 필요

### DART API
- **제공 데이터**: 기업 재무제표, 공시 정보
- **API 문서**: https://opendart.fss.or.kr/

## 트러블슈팅

### 문제: API 응답이 느림
```bash
# 네트워크 상태 확인
ping apis.data.go.kr

# API 키 테스트
python3 tests/test_data_go_kr.py
```

### 문제: 데이터베이스 연결 오류
```bash
# DB 연결 테스트
python3 -c "from collect_data_go_kr import get_db_connection; conn = get_db_connection(); print('연결 성공'); conn.close()"
```

### 문제: 히스토리 수집 중단됨
- 스크립트를 다시 실행하면 됩니다
- UPSERT 패턴으로 중복 데이터 자동 처리
- 이미 수집된 데이터는 업데이트되고, 없는 데이터만 추가됩니다

## 주의사항

1. **API 키 관리**
   - `.env` 파일에 API 키를 안전하게 보관하세요
   - `.env` 파일은 절대 Git에 커밋하지 마세요

2. **데이터베이스 백업**
   - 대량 데이터 수집 전에 데이터베이스 백업을 권장합니다
   ```bash
   pg_dump -h 124.54.191.68 -p 5433 -U stock_user -d stock_analysis > backup.sql
   ```

3. **API 호출 한도**
   - 공공데이터포털 API 일일 호출 한도를 확인하세요
   - 스크립트는 1초당 1~2회 정도로 호출 제한합니다

4. **네트워크 안정성**
   - 히스토리 수집은 장시간 실행되므로 안정적인 네트워크 환경이 필요합니다
   - 중단 시 재시작하면 이어서 수집 가능합니다

## 유지보수

### 일일 데이터 업데이트
최신 데이터를 유지하려면 매일 다음 스크립트를 실행하세요:
```bash
python3 collect_data_go_kr.py  # 주식 데이터
python3 collect_etf_go_kr.py   # ETF 데이터
```

또는 크론탭으로 자동화하세요.

### 정기 데이터 검증
주기적으로 데이터 상태를 확인하세요:
```bash
python3 check_db_status.py
python3 verify_new_fields.py
```

## 라이선스

이 프로젝트는 개인 프로젝트입니다.
