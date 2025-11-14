"""데이터베이스 핸들러 - Phase 3 개선"""
import psycopg2
import os
from dotenv import load_dotenv
from common.logger import get_logger, log_exception, log_db_operation

# 로거 설정
logger = get_logger(__name__, 'db_handler.log')

load_dotenv('../.env')

def get_db_connection():
    """데이터베이스 연결 생성 (에러 처리 추가)"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        logger.debug(f"DB 연결 성공: {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")
        return conn
    except Exception as e:
        log_exception(logger, f"DB 연결 실패: {str(e)}")
        raise

def save_stock(stock_code, stock_name, market_type, asset_type='STOCK'):
    """종목 정보 저장 (에러 처리 및 로깅 추가)"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO stocks (stock_code, stock_name, market_type, asset_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_code) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                asset_type = EXCLUDED.asset_type
        """, (stock_code, stock_name, market_type, asset_type))
        conn.commit()

        log_db_operation(logger, "INSERT/UPDATE", "stocks", count=1, success=True)
        logger.info(f"✅ {stock_name} ({stock_code}) 저장 완료")

    except Exception as e:
        if conn:
            conn.rollback()
        log_exception(logger, f"종목 저장 실패: {stock_name} ({stock_code})")
        log_db_operation(logger, "INSERT/UPDATE", "stocks", success=False, error=str(e))
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
if __name__ == "__main__":
    save_stock("005930", "삼성전자", "KOSPI")
