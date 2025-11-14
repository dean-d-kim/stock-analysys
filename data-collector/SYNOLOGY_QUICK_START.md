# 시놀로지 NAS 빠른 시작 가이드

GitHub를 통해 코드를 이미 받았다면, 다음 단계만 진행하면 됩니다.

## 1. 시놀로지 SSH 접속

```bash
ssh admin@124.54.191.68
```

## 2. 기존 코드 위치 확인

GitHub에서 받은 프로젝트 위치로 이동:
```bash
# 예: /volume1/docker/stock-analysis/stock-analysis-project
cd /volume1/docker/stock-analysis/stock-analysis-project
```

## 3. 최신 코드 받기

```bash
git pull origin main
```

## 4. Python 패키지 설치

```bash
cd data-collector

# pip 업그레이드
python3 -m pip install --upgrade pip

# 패키지 설치
python3 -m pip install --user -r requirements.txt
```

## 5. 환경 변수 설정

`.env` 파일이 없다면 생성:
```bash
# .env 파일 생성
cp .env.example .env

# nano 또는 vi로 편집
nano .env
```

필수 환경 변수:
```env
# 데이터베이스
DB_HOST=124.54.191.68
DB_PORT=5433
DB_NAME=stock_analysis
DB_USER=stock_user
DB_PASSWORD=StockDB2025!

# API 키
DATA_GO_KR_API_KEY=your_api_key_here
DART_API_KEY=your_dart_api_key_here
```

## 6. 테스트 실행

```bash
# KOSPI/KOSDAQ 데이터 수집 테스트
python3 collect_data_go_kr.py

# ETF 데이터 수집 테스트
python3 collect_etf_go_kr.py
```

## 7. Task Scheduler 설정

DSM > 제어판 > 작업 스케줄러에서 다음 작업 생성:

### 작업 1: KOSPI/KOSDAQ (오전 2시)
- **일정**: 매일 02:00
- **스크립트**:
```bash
#!/bin/bash
cd /volume1/docker/stock-analysis/stock-analysis-project/data-collector
/usr/bin/python3 collect_data_go_kr.py >> /tmp/stock_update_2am.log 2>&1
```

### 작업 2: KOSPI/KOSDAQ (오후 5시)
- **일정**: 매일 17:00
- **스크립트**:
```bash
#!/bin/bash
cd /volume1/docker/stock-analysis/stock-analysis-project/data-collector
/usr/bin/python3 collect_data_go_kr.py >> /tmp/stock_update_5pm.log 2>&1
```

### 작업 3: ETF (오후 5시)
- **일정**: 매일 17:00
- **스크립트**:
```bash
#!/bin/bash
cd /volume1/docker/stock-analysis/stock-analysis-project/data-collector
/usr/bin/python3 collect_etf_go_kr.py >> /tmp/etf_update_5pm.log 2>&1
```

## 8. 로그 확인

```bash
# 실시간 로그 확인
tail -f /tmp/stock_update_5pm.log
tail -f /tmp/etf_update_5pm.log

# 에러 로그 확인
tail -f /volume1/docker/stock-analysis/stock-analysis-project/data-collector/common/logs/errors.log
```

## 9. 데이터 수집 현황 확인

웹 브라우저에서:
```
http://124.54.191.68:3000/api/admin/stats/data-range
```

또는 시놀로지에서:
```bash
curl http://124.54.191.68:3000/api/admin/stats/data-range | python3 -m json.tool
```

## 트러블슈팅

### 문제: psycopg2 설치 실패
```bash
python3 -m pip uninstall psycopg2 psycopg2-binary
python3 -m pip install --user psycopg2-binary
```

### 문제: 권한 오류
```bash
chmod +x *.py
chmod 600 .env
```

### 문제: DB 연결 실패
```bash
# DB 연결 테스트
python3 -c "
import psycopg2
conn = psycopg2.connect(
    host='124.54.191.68',
    port=5433,
    database='stock_analysis',
    user='stock_user',
    password='StockDB2025!'
)
print('연결 성공!')
conn.close()
"
```

## 주의사항

1. **.env 파일 보안**: `.env` 파일에 민감한 정보가 있으므로 권한을 `600`으로 설정
2. **로그 파일 용량**: 로그 파일이 너무 커지지 않도록 주기적으로 확인
3. **cron 실행 시간**: 한국 시간 기준으로 설정 (DSM의 시간대 확인)

## 업데이트 방법

코드가 업데이트되면:
```bash
cd /volume1/docker/stock-analysis/stock-analysis-project
git pull origin main
cd data-collector
python3 -m pip install --user -r requirements.txt
```
