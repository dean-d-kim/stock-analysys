#!/bin/bash

# 시놀로지 NAS 설정
SYNOLOGY_HOST="124.54.191.68"
SYNOLOGY_USER="admin"
SYNOLOGY_PATH="/volume1/docker/stock-analysis"

echo "=================================="
echo "시놀로지 NAS 데이터 수집 배포 스크립트"
echo "=================================="

# 1. 시놀로지 연결 테스트
echo ""
echo "[1/5] 시놀로지 연결 테스트..."
if ssh -o ConnectTimeout=5 ${SYNOLOGY_USER}@${SYNOLOGY_HOST} "echo 'Connected'" > /dev/null 2>&1; then
    echo "✅ 연결 성공"
else
    echo "❌ 연결 실패. SSH 접속 설정을 확인하세요."
    echo "   ssh ${SYNOLOGY_USER}@${SYNOLOGY_HOST}"
    exit 1
fi

# 2. 디렉토리 생성
echo ""
echo "[2/5] 디렉토리 생성..."
ssh ${SYNOLOGY_USER}@${SYNOLOGY_HOST} "mkdir -p ${SYNOLOGY_PATH}/data-collector/common/logs && mkdir -p ${SYNOLOGY_PATH}/logs"
echo "✅ 디렉토리 생성 완료"

# 3. 파일 전송
echo ""
echo "[3/5] 파일 전송 중..."

# 메인 스크립트 파일들
scp collect_data_go_kr.py ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/
scp collect_etf_go_kr.py ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/
scp collect_2025_data.py ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/
scp collect_etf_historical.py ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/

# 환경 변수 및 설정 파일
scp .env ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/
scp requirements.txt ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/

# common 폴더
scp -r common/ ${SYNOLOGY_USER}@${SYNOLOGY_HOST}:${SYNOLOGY_PATH}/data-collector/

echo "✅ 파일 전송 완료"

# 4. Python 패키지 설치
echo ""
echo "[4/5] Python 패키지 설치..."
ssh ${SYNOLOGY_USER}@${SYNOLOGY_HOST} "cd ${SYNOLOGY_PATH}/data-collector && python3 -m pip install --user -r requirements.txt"
echo "✅ 패키지 설치 완료"

# 5. 권한 설정
echo ""
echo "[5/5] 권한 설정..."
ssh ${SYNOLOGY_USER}@${SYNOLOGY_HOST} "chmod +x ${SYNOLOGY_PATH}/data-collector/*.py && chmod 600 ${SYNOLOGY_PATH}/data-collector/.env && chmod 777 ${SYNOLOGY_PATH}/logs"
echo "✅ 권한 설정 완료"

# 테스트 실행
echo ""
echo "=================================="
echo "배포 완료!"
echo "=================================="
echo ""
echo "다음 단계:"
echo "1. 시놀로지 DSM > 제어판 > 작업 스케줄러로 이동"
echo "2. 다음 작업들을 생성:"
echo ""
echo "   작업 1: KOSPI/KOSDAQ (오전 2시)"
echo "   스크립트: cd ${SYNOLOGY_PATH}/data-collector && /usr/bin/python3 collect_data_go_kr.py >> ${SYNOLOGY_PATH}/logs/stock_update_2am.log 2>&1"
echo ""
echo "   작업 2: KOSPI/KOSDAQ (오후 5시)"
echo "   스크립트: cd ${SYNOLOGY_PATH}/data-collector && /usr/bin/python3 collect_data_go_kr.py >> ${SYNOLOGY_PATH}/logs/stock_update_5pm.log 2>&1"
echo ""
echo "   작업 3: ETF (오후 5시)"
echo "   스크립트: cd ${SYNOLOGY_PATH}/data-collector && /usr/bin/python3 collect_etf_go_kr.py >> ${SYNOLOGY_PATH}/logs/etf_update_5pm.log 2>&1"
echo ""
echo "3. 수동 테스트:"
echo "   ssh ${SYNOLOGY_USER}@${SYNOLOGY_HOST}"
echo "   cd ${SYNOLOGY_PATH}/data-collector"
echo "   python3 collect_data_go_kr.py"
echo ""
