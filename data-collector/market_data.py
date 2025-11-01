import requests
import os
from dotenv import load_dotenv
from auth import get_token

load_dotenv('../.env')

APP_KEY = os.getenv('KIS_APP_KEY')
BASE_URL = "https://openapi.koreainvestment.com:9443"

def get_stock_price(stock_code="005930"):
    """삼성전자 현재가 조회"""
    token = get_token()
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": os.getenv('KIS_APP_SECRET'),
        "tr_id": "FHKST01010100"
    }
    
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": stock_code
    }
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()['output']
        print(f"종목: {data['prdy_vrss']}")
        print(f"현재가: {data['stck_prpr']}원")
        return data
    else:
        print("조회 실패:", response.text)
        return None

if __name__ == "__main__":
    get_stock_price()
