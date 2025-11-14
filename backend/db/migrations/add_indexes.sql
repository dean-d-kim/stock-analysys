-- 성능 최적화를 위한 인덱스 추가
-- Phase 2 리팩토링

-- 1. daily_prices 테이블 인덱스
-- stock_code와 trade_date 조합 인덱스 (이미 UNIQUE 제약으로 존재할 수 있음)
CREATE INDEX IF NOT EXISTS idx_daily_prices_stock_date
ON daily_prices(stock_code, trade_date DESC);

-- trade_date만 사용하는 쿼리를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_daily_prices_trade_date
ON daily_prices(trade_date DESC);

-- 2. stocks 테이블 인덱스
-- market_type 필터링을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_stocks_market_type
ON stocks(market_type);

-- market_cap 정렬을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_stocks_market_cap
ON stocks(market_cap DESC NULLS LAST);

-- listed_shares (시가총액 계산에 사용)
CREATE INDEX IF NOT EXISTS idx_stocks_listed_shares
ON stocks(listed_shares);

-- 3. 복합 인덱스
-- market_type과 market_cap을 함께 사용하는 쿼리를 위한 복합 인덱스
CREATE INDEX IF NOT EXISTS idx_stocks_market_type_market_cap
ON stocks(market_type, market_cap DESC NULLS LAST);

-- stock_name 검색을 위한 인덱스 (LIKE 쿼리 최적화)
CREATE INDEX IF NOT EXISTS idx_stocks_stock_name_trgm
ON stocks USING gin(stock_name gin_trgm_ops);

-- stock_code 검색을 위한 인덱스 (이미 PRIMARY KEY로 존재)

-- 4. 통계 정보 업데이트
ANALYZE daily_prices;
ANALYZE stocks;
