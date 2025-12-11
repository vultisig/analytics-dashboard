-- Migration: Add historical_prices and ingestion_errors tables
-- Date: 2025-11-20
-- Purpose: Support CoinGecko price fetching with cache and error logging

-- Table for caching historical token prices from CoinGecko
CREATE TABLE IF NOT EXISTS historical_prices (
    id SERIAL PRIMARY KEY,
    token_id VARCHAR(50) NOT NULL,  -- CoinGecko token ID (e.g., 'thorchain', 'maya-protocol')
    date DATE NOT NULL,              -- Price date (YYYY-MM-DD)
    price_usd NUMERIC(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(token_id, date)
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_historical_prices_token_date ON historical_prices(token_id, date);

-- Table for logging ingestion errors that need reprocessing
CREATE TABLE IF NOT EXISTS ingestion_errors (
    id SERIAL PRIMARY KEY,
    tx_hash VARCHAR(100) NOT NULL,
    source VARCHAR(20) NOT NULL,     -- 'thorchain', 'mayachain', etc.
    error_type VARCHAR(50) NOT NULL, -- 'missing_price', 'invalid_metadata', etc.
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    raw_data JSONB,                  -- Store original transaction for re-ingestion
    UNIQUE(tx_hash, source)
);

-- Indexes for error table queries
CREATE INDEX IF NOT EXISTS idx_ingestion_errors_retry ON ingestion_errors(error_type, retry_count);
CREATE INDEX IF NOT EXISTS idx_ingestion_errors_source ON ingestion_errors(source);
CREATE INDEX IF NOT EXISTS idx_ingestion_errors_created ON ingestion_errors(created_at DESC);

-- Add comments for documentation
COMMENT ON TABLE historical_prices IS 'Cache for CoinGecko historical price data to minimize API calls';
COMMENT ON TABLE ingestion_errors IS 'Log of failed transaction ingestions with retry metadata';
