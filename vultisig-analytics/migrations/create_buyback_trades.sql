CREATE TABLE IF NOT EXISTS buyback_trades (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    tx_hash VARCHAR(66) UNIQUE NOT NULL,
    block_number BIGINT NOT NULL,
    usdc_spent NUMERIC(38, 6) NOT NULL,
    vult_bought NUMERIC(38, 18) NOT NULL,
    price NUMERIC(38, 18) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buyback_trades_date ON buyback_trades(date DESC);
