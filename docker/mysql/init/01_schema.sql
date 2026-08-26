CREATE TABLE IF NOT EXISTS trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    side ENUM('buy', 'sell') NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    event_time DATETIME(3) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_client_symbol (client_id, symbol)
);

-- Stage 1: naive upsert target (quantity/avg_buy_price are not yet correct
-- weighted-average math -- that lands in Stage 7, under a row lock in Stage 8).
CREATE TABLE IF NOT EXISTS positions (
    client_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL DEFAULT 0,
    avg_buy_price DECIMAL(20, 8) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id, symbol)
);

-- Stage 13: official EOD closing prices -- an independent source of truth
-- from the live tick feed, populated by a separate process (simulated by
-- scripts/seed_eod_prices.py), never written to by anything in the live
-- pipeline.
CREATE TABLE IF NOT EXISTS eod_closing_prices (
    symbol VARCHAR(16) NOT NULL,
    close_date DATE NOT NULL,
    close_price DECIMAL(20, 8) NOT NULL,
    PRIMARY KEY (symbol, close_date)
);
