# 시놀로지 NAS 데이터 수집 설정 가이드

## 1. 사전 준비

### 시놀로지 패키지 설치
1. **Package Center**에서 설치:
   - Python 3.9 (또는 최신 버전)
   - Git Server (선택사항)

### SSH 접속 활성화
1. 제어판 > 터미널 및 SNMP > 터미널 탭
2. "SSH 서비스 활성화" 체크

## 2. 파일 업로드

### 필요한 파일/폴더
```
data-collector/
├── .env                          # 환경 변수 (DB 접속 정보, API 키)
├── requirements.txt              # Python 패키지 의존성
├── collect_data_go_kr.py         # KOSPI/KOSDAQ 데이터 수집
├── collect_etf_go_kr.py          # ETF 데이터 수집
├── collect_2025_data.py          # 2025년 데이터 수집
├── collect_etf_historical.py     # ETF 과거 데이터 수집
└── common/                       # 공통 모듈
    ├── __init__.py
    ├── config.py
    ├── logger.py
    └── db_utils.py
```

### 업로드 방법
**방법 1: SCP 사용 (로컬 Mac에서)**
```bash
# 데이터 수집 폴더 전체 복사
scp -r /Users/ddkim/stock-analysis-project/data-collector/ admin@124.54.191.68:/volume1/docker/stock-analysis/
```

**방법 2: File Station 사용**
1. File Station 웹 UI에서 직접 업로드
2. `/volume1/docker/stock-analysis/data-collector/` 경로에 업로드

## 3. Python 환경 설정

### SSH로 시놀로지 접속
```bash
ssh admin@124.54.191.68
```

### Python 패키지 설치
```bash
cd /volume1/docker/stock-analysis/data-collector

# pip 업그레이드
python3 -m pip install --upgrade pip

# 필요한 패키지 설치
python3 -m pip install --user psycopg2-binary requests python-dotenv

# 또는 requirements.txt 사용
python3 -m pip install --user -r requirements.txt
```

### 환경 변수 설정 확인
```bash
# .env 파일 확인
cat .env
```

## 4. 수동 테스트

### 스크립트 실행 권한 부여
```bash
chmod +x collect_data_go_kr.py
chmod +x collect_etf_go_kr.py
```

### 테스트 실행
```bash
# KOSPI/KOSDAQ 데이터 수집 테스트
python3 collect_data_go_kr.py

# ETF 데이터 수집 테스트
python3 collect_etf_go_kr.py
```

## 5. Task Scheduler 설정

### 제어판 > 작업 스케줄러

**작업 1: KOSPI/KOSDAQ 데이터 수집 (오전 2시)**
- 작업: 사용자 정의 스크립트 실행
- 사용자: root
- 일정: 매일 02:00
- 스크립트:
```bash
#!/bin/bash
cd /volume1/docker/stock-analysis/data-collector
/usr/bin/python3 collect_data_go_kr.py >> /volume1/docker/stock-analysis/logs/stock_update_2am.log 2>&1
```

**작업 2: KOSPI/KOSDAQ 데이터 수집 (오후 5시)**
- 작업: 사용자 정의 스크립트 실행
- 사용자: root
- 일정: 매일 17:00
- 스크립트:
```bash
#!/bin/bash
cd /volume1/docker/stock-analysis/data-collector
/usr/bin/python3 collect_data_go_kr.py >> /volume1/docker/stock-analysis/logs/stock_update_5pm.log 2>&1
```

**작업 3: ETF 데이터 수집 (오후 5시)**
- 작업: 사용자 정의 스크립트 실행
- 사용자: root
- 일정: 매일 17:00
- 스크립트:
```bash
#!/bin/bash
cd /volume1/docker/stock-analysis/data-collector
/usr/bin/python3 collect_etf_go_kr.py >> /volume1/docker/stock-analysis/logs/etf_update_5pm.log 2>&1
```

## 6. 로그 디렉토리 생성

```bash
mkdir -p /volume1/docker/stock-analysis/logs
chmod 777 /volume1/docker/stock-analysis/logs
```

## 7. 모니터링 및 유지보수

### 로그 확인
```bash
# 최근 실행 로그 확인
tail -f /volume1/docker/stock-analysis/logs/stock_update_5pm.log
tail -f /volume1/docker/stock-analysis/logs/etf_update_5pm.log

# 에러 로그 확인
tail -f /volume1/docker/stock-analysis/data-collector/common/logs/errors.log
```

### 데이터 수집 상태 확인
```bash
# API 통해 확인
curl http://124.54.191.68:3000/api/admin/stats/data-range
```

## 8. 트러블슈팅

### 문제: psycopg2 설치 실패
**해결:**
```bash
# psycopg2-binary 대신 사용
python3 -m pip install --user psycopg2-binary
```

### 문제: 권한 오류
**해결:**
```bash
# 소유권 변경
sudo chown -R admin:users /volume1/docker/stock-analysis/data-collector
chmod -R 755 /volume1/docker/stock-analysis/data-collector
```

### 문제: 환경 변수를 찾을 수 없음
**해결:**
```bash
# .env 파일 경로 확인
ls -la /volume1/docker/stock-analysis/data-collector/.env

# 스크립트에서 절대 경로 사용하도록 수정
```

## 9. 보안 고려사항

### .env 파일 권한 설정
```bash
chmod 600 /volume1/docker/stock-analysis/data-collector/.env
```

### 방화벽 설정
- 외부에서 5433 포트 접근 가능하도록 설정 확인
- 필요시 IP 화이트리스트 설정

## 10. 백업 설정

### Hyper Backup 설정
1. 제어판 > Hyper Backup
2. 데이터 백업 작업 생성
3. `/volume1/docker/stock-analysis/data-collector` 포함
4. 일일 백업 스케줄 설정

## 참고사항

- 시놀로지는 24/7 운영되므로 macOS cron 문제 해결됨
- 전력 소모가 낮고 안정적
- Docker 컨테이너로 실행하는 것도 고려 가능
- Task Scheduler의 실행 결과는 이메일로 알림 설정 가능
