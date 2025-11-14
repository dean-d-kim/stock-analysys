import requests
import os
from dotenv import load_dotenv

load_dotenv('../.env')

DART_API_KEY = os.getenv('DART_API_KEY')
BASE_URL = "https://opendart.fss.or.kr/api"

def get_company_info(stock_code):
    """기업 정보 조회"""
    url = f"{BASE_URL}/company.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': stock_code
    }
    response = requests.get(url, params=params)
    return response.json()

def get_financial_statement(corp_code, year, quarter):
    """재무제표 조회"""
    url = f"{BASE_URL}/fnlttSinglAcntAll.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': corp_code,
        'bsns_year': year,
        'reprt_code': f'{quarter}Q'  # 11013(1분기), 11012(반기), 11014(3분기), 11011(사업보고서)
    }
    response = requests.get(url, params=params)
    return response.json()

# 테스트
if __name__ == "__main__":
    print("DART API 연결 테스트")