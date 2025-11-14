import requests
from bs4 import BeautifulSoup
from db_handler import save_stock
import time

def is_etf(name, code):
    """ETF 판별"""
    etf_keywords = ['KODEX', 'TIGER', 'ARIRANG', 'KBSTAR', 'HANARO', 'KOSEF', 
                    'KINDEX', 'TIMEFOLIO', 'SOL', 'TRUE', 'ACE']
    
    for keyword in etf_keywords:
        if keyword in name:
            return True
    
    if code.startswith('2'):
        return True
    
    return False

def fetch_all_stocks(market='KOSPI'):
    """전체 종목 수집"""
    stocks = []
    page = 1
    
    while True:
        if market == 'KOSPI':
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={page}"
        else:
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={page}"
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        table = soup.select('table.type_2 tr')
        found = False
        
        for row in table[2:]:
            cells = row.select('td')
            if len(cells) < 2:
                continue
            
            name_tag = cells[1].select_one('a')
            if name_tag:
                name = name_tag.text.strip()
                href = name_tag['href']
                code = href.split('code=')[1][:6]
                
                asset = 'ETF' if is_etf(name, code) else 'STOCK'
                stocks.append((code, name, asset))
                found = True
        
        if not found:
            break
        
        print(f"{market} {page}페이지 수집 완료 (총 {len(stocks)}개)")
        page += 1
        time.sleep(0.5)
    
    return stocks

if __name__ == "__main__":
    print("📊 KOSPI 전종목 수집 중...")
    kospi = fetch_all_stocks('KOSPI')
    
    print("\n📊 KOSDAQ 전종목 수집 중...")
    kosdaq = fetch_all_stocks('KOSDAQ')
    
    print(f"\n총 {len(kospi) + len(kosdaq)}개 종목 DB 저장 중...")
    
    for code, name, asset in kospi:
        save_stock(code, name, "KOSPI", asset)
    
    for code, name, asset in kosdaq:
        save_stock(code, name, "KOSDAQ", asset)
    
    print(f"✅ KOSPI {len(kospi)}개, KOSDAQ {len(kosdaq)}개 저장 완료!")