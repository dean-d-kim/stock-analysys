"""데이터베이스 유틸리티 모듈"""
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from .config import DB_CONFIG
from .logger import get_logger

logger = get_logger(__name__)

# 커넥션 풀 생성
connection_pool = None

def init_connection_pool(min_conn=1, max_conn=10):
    """데이터베이스 커넥션 풀 초기화"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            min_conn,
            max_conn,
            **DB_CONFIG
        )
        logger.info("데이터베이스 커넥션 풀 초기화 성공")
        return connection_pool
    except Exception as e:
        logger.error(f"커넥션 풀 초기화 실패: {e}")
        raise

@contextmanager
def get_db_connection():
    """데이터베이스 연결 컨텍스트 매니저"""
    global connection_pool

    if connection_pool is None:
        init_connection_pool()

    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

@contextmanager
def get_db_cursor(commit=True):
    """데이터베이스 커서 컨텍스트 매니저"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"데이터베이스 오류: {e}")
            raise
        finally:
            cursor.close()

def execute_query(query, params=None, fetch=False):
    """쿼리 실행 헬퍼 함수"""
    with get_db_cursor() as cursor:
        cursor.execute(query, params)
        if fetch:
            return cursor.fetchall()
        return cursor.rowcount

def upsert_stock(stock_data):
    """종목 정보 UPSERT"""
    query = """
        INSERT INTO stocks (
            stock_code, stock_name, market_type, asset_type,
            isin_code, listed_shares, market_cap,
            nav, net_asset_total, base_index_name, base_index_close
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_code)
        DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            market_type = EXCLUDED.market_type,
            asset_type = EXCLUDED.asset_type,
            isin_code = EXCLUDED.isin_code,
            listed_shares = EXCLUDED.listed_shares,
            market_cap = EXCLUDED.market_cap,
            nav = EXCLUDED.nav,
            net_asset_total = EXCLUDED.net_asset_total,
            base_index_name = EXCLUDED.base_index_name,
            base_index_close = EXCLUDED.base_index_close
    """
    return execute_query(query, tuple(stock_data.values()))

def upsert_daily_price(price_data):
    """일별 시세 UPSERT"""
    query = """
        INSERT INTO daily_prices (
            stock_code, trade_date, open_price, high_price, low_price,
            close_price, volume, vs, change_rate, trading_value
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_code, trade_date)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            vs = EXCLUDED.vs,
            change_rate = EXCLUDED.change_rate,
            trading_value = EXCLUDED.trading_value
    """
    return execute_query(query, tuple(price_data.values()))
