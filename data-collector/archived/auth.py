import requests
import os
from dotenv import load_dotenv

load_dotenv('../.env')

APP_KEY = os.getenv('KIS_APP_KEY')
APP_SECRET = os.getenv('KIS_APP_SECRET')
BASE_URL = "https://openapi.koreainvestment.com:9443"

def get_token():
    url = f"{BASE_URL}/oauth2/tokenP"
    data = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print("토큰 발급 실패:", response.text)
        return None

if __name__ == "__main__":
    token = get_token()
    if token:
        print("✅ 토큰 발급 성공!")
        print(f"Token: {token[:50]}...")
    else:
        print("❌ 토큰 발급 실패")
