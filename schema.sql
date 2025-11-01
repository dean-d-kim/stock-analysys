CREATE TABLE stocks (
    stock_code VARCHAR(10) PRIMARY KEY,
    stock_name VARCHAR(100) NOT NULL,
    market_type VARCHAR(10),
    sector VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE daily_prices (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) REFERENCES stocks(stock_code),
    trade_date DATE NOT NULL,
    open_price INTEGER,
    high_price INTEGER,
    low_price INTEGER,
    close_price INTEGER,
    volume BIGINT,
    trading_value BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code, trade_date)
);

CREATE TABLE realtime_prices (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) REFERENCES stocks(stock_code),
    current_price INTEGER,
    change_rate DECIMAL(5,2),
    volume BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_daily_prices_date ON daily_prices(trade_date);
CREATE INDEX idx_daily_prices_stock ON daily_prices(stock_code);
CREATE INDEX idx_realtime_timestamp ON realtime_prices(timestamp);
