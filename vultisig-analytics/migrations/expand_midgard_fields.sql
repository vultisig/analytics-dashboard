-- Migration: Expand swaps table with complete Midgard field logging
-- Created: 2025-12-09
-- Purpose: Store ALL fields from Midgard API responses for comprehensive tracking

-- Add new columns to swaps table
ALTER TABLE swaps
    ADD COLUMN IF NOT EXISTS in_address TEXT,
    ADD COLUMN IF NOT EXISTS in_tx_id TEXT,
    ADD COLUMN IF NOT EXISTS in_amount_raw TEXT,
    ADD COLUMN IF NOT EXISTS out_addresses JSONB,
    ADD COLUMN IF NOT EXISTS out_tx_ids TEXT[],
    ADD COLUMN IF NOT EXISTS out_heights BIGINT[],
    ADD COLUMN IF NOT EXISTS affiliate_addresses TEXT[],
    ADD COLUMN IF NOT EXISTS affiliate_fees_bps INTEGER[],
    ADD COLUMN IF NOT EXISTS metadata_complete JSONB,
    ADD COLUMN IF NOT EXISTS in_price_usd NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS out_price_usd NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS network_fees_raw JSONB,
    ADD COLUMN IF NOT EXISTS pools_used TEXT[],
    ADD COLUMN IF NOT EXISTS swap_status TEXT,
    ADD COLUMN IF NOT EXISTS swap_type TEXT,
    ADD COLUMN IF NOT EXISTS memo TEXT;

-- Add indexes for new fields
CREATE INDEX IF NOT EXISTS idx_swaps_in_address ON swaps(in_address);
CREATE INDEX IF NOT EXISTS idx_swaps_affiliate_addresses ON swaps USING GIN(affiliate_addresses);
CREATE INDEX IF NOT EXISTS idx_swaps_status ON swaps(swap_status);
CREATE INDEX IF NOT EXISTS idx_swaps_type ON swaps(swap_type);

-- Create asset_decimals table for decimal conversion
CREATE TABLE IF NOT EXISTS asset_decimals (
    id SERIAL PRIMARY KEY,
    asset_symbol TEXT NOT NULL,
    chain TEXT NOT NULL,
    decimal_places INTEGER NOT NULL,
    contract_address TEXT,
    full_asset_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_asset_decimals_symbol ON asset_decimals(asset_symbol);
CREATE INDEX IF NOT EXISTS idx_asset_decimals_chain ON asset_decimals(chain);
CREATE INDEX IF NOT EXISTS idx_asset_decimals_full_id ON asset_decimals(full_asset_id);

-- Create ingestion_errors table if it doesn't exist
CREATE TABLE IF NOT EXISTS ingestion_errors (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT,
    source TEXT NOT NULL,
    error_type TEXT,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    raw_data JSONB,
    UNIQUE(tx_hash, source)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_errors_source ON ingestion_errors(source);
CREATE INDEX IF NOT EXISTS idx_ingestion_errors_type ON ingestion_errors(error_type);
CREATE INDEX IF NOT EXISTS idx_ingestion_errors_retry ON ingestion_errors(error_type, retry_count);

COMMENT ON TABLE asset_decimals IS 'Cached asset decimal information from Midgard Pools API';
COMMENT ON COLUMN asset_decimals.full_asset_id IS 'Complete asset identifier (e.g., AVAX.AVAX, ETH.USDC-0x...)';
COMMENT ON COLUMN asset_decimals.decimal_places IS 'Number of decimal places for amount conversion (e.g., 18 for ETH, 6 for USDC)';

COMMENT ON COLUMN swaps.in_address IS 'Transaction initiator address';
COMMENT ON COLUMN swaps.in_tx_id IS 'Incoming transaction ID';
COMMENT ON COLUMN swaps.in_amount_raw IS 'Raw token amount before decimal conversion';
COMMENT ON COLUMN swaps.out_addresses IS 'Array of output addresses with coins and affiliate flags';
COMMENT ON COLUMN swaps.affiliate_addresses IS 'Array of affiliate codes (vi, va, v0) extracted from metadata';
COMMENT ON COLUMN swaps.affiliate_fees_bps IS 'Array of affiliate fees in basis points (BPS), corresponding to affiliate_addresses';
COMMENT ON COLUMN swaps.metadata_complete IS 'Complete metadata.swap object from Midgard API';
COMMENT ON COLUMN swaps.pools_used IS 'Array of pool identifiers involved in swap';
COMMENT ON COLUMN swaps.swap_status IS 'Swap status: success, pending, refunded, etc.';
COMMENT ON COLUMN swaps.swap_type IS 'Transaction type: swap, add, withdraw, etc.';
