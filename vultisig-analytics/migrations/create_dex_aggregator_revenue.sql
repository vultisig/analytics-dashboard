-- Create new table for DEX aggregator revenue tracking
-- This table stores data from Arkham API (actual fees) and blockchain (volumes)

CREATE TABLE IF NOT EXISTS dex_aggregator_revenue (
  id SERIAL PRIMARY KEY,
  tx_hash VARCHAR(66) UNIQUE NOT NULL,
  chain VARCHAR(50) NOT NULL,
  protocol VARCHAR(50) NOT NULL, -- '1inch', 'paraswap', 'cowswap', 'other'
  timestamp TIMESTAMP NOT NULL,
  
  -- Actual data from Arkham (ground truth)
  actual_fee_usd NUMERIC(20,8) NOT NULL,
  fee_token_symbol VARCHAR(100),
  fee_token_address VARCHAR(42),
  fee_amount_raw VARCHAR(100),
  
  -- Swap volume (from blockchain or API)
  swap_volume_usd NUMERIC(20,8),
  token_in_symbol VARCHAR(100),
  token_in_address VARCHAR(42),
  token_out_symbol VARCHAR(100),
  token_out_address VARCHAR(42),
  amount_in NUMERIC(30,18),
  amount_out NUMERIC(30,18),
  
  -- Metadata
  block_number BIGINT,
  from_address VARCHAR(42),
  to_address VARCHAR(42),
  
  -- Data source tracking
  fee_data_source VARCHAR(20) DEFAULT 'arkham', -- 'arkham', 'calculated'
  volume_data_source VARCHAR(20), -- 'blockchain', '1inch_api', 'estimated', null
  
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_dex_revenue_timestamp ON dex_aggregator_revenue(timestamp);
CREATE INDEX IF NOT EXISTS idx_dex_revenue_chain ON dex_aggregator_revenue(chain);
CREATE INDEX IF NOT EXISTS idx_dex_revenue_protocol ON dex_aggregator_revenue(protocol);
CREATE INDEX IF NOT EXISTS idx_dex_revenue_tx_hash ON dex_aggregator_revenue(tx_hash);

-- Create composite index for dashboard queries
CREATE INDEX IF NOT EXISTS idx_dex_revenue_query ON dex_aggregator_revenue(protocol, chain, timestamp);

-- Add update trigger
CREATE OR REPLACE FUNCTION update_dex_revenue_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_dex_revenue_timestamp
BEFORE UPDATE ON dex_aggregator_revenue
FOR EACH ROW
EXECUTE FUNCTION update_dex_revenue_timestamp();

-- Add comments for documentation
COMMENT ON TABLE dex_aggregator_revenue IS 'DEX aggregator fee revenue and swap volumes - primary data source is Arkham API';
COMMENT ON COLUMN dex_aggregator_revenue.actual_fee_usd IS 'Actual fee received (from Arkham historicalUSD) - ground truth';
COMMENT ON COLUMN dex_aggregator_revenue.swap_volume_usd IS 'Total swap volume in USD (from blockchain data or API)';
COMMENT ON COLUMN dex_aggregator_revenue.protocol IS 'Identified DEX aggregator protocol';
COMMENT ON COLUMN dex_aggregator_revenue.fee_data_source IS 'Source of fee data (arkham = actual, calculated = from 1inch API)';
COMMENT ON COLUMN dex_aggregator_revenue.volume_data_source IS 'Source of volume data (blockchain, 1inch_api, estimated, or null if unavailable)';
