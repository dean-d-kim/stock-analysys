"""환경 설정 모듈"""
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

# 데이터베이스 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# API 설정
DATA_GO_KR_API_KEY = os.getenv('DATA_GO_KR_API_KEY')
DART_API_KEY = os.getenv('DART_API_KEY')

# API 엔드포인트
DATA_GO_KR_BASE_URL = 'http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService'
DART_BASE_URL = 'https://opendart.fss.or.kr/api'

# 로그 설정
LOG_DIR = os.path.join(os.path.dirname(__file__), '../logs')
os.makedirs(LOG_DIR, exist_ok=True)
