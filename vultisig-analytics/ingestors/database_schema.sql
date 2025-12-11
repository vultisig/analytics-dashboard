-- database_schema.sql

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Main swaps table (fixed schema)
CREATE TABLE swaps (
    -- Composite primary key including timestamp for TimescaleDB
    timestamp TIMESTAMPTZ NOT NULL,
    tx_hash VARCHAR(255) NOT NULL,
    source VARCHAR(20) NOT NULL,
    
    -- Additional fields
    id BIGSERIAL,
    date_only DATE NOT NULL,
    
    -- Transaction details
    block_height BIGINT,
    
    -- User information
    user_address VARCHAR(255) NOT NULL,
    user_timezone VARCHAR(50), -- derived from user activity patterns
    platform VARCHAR(50), -- iOS, Android, Web, etc.
    
    -- Input token
    in_asset VARCHAR(100) NOT NULL,
    in_amount NUMERIC(38, 18) NOT NULL,
    in_amount_usd NUMERIC(20, 8),
    
    -- Output token
    out_asset VARCHAR(100) NOT NULL,
    out_amount NUMERIC(38, 18) NOT NULL,
    out_amount_usd NUMERIC(20, 8),
    
    -- Fees and metrics
    total_fee_usd NUMERIC(20, 8),
    network_fee_usd NUMERIC(20, 8),
    liquidity_fee_usd NUMERIC(20, 8),
    affiliate_fee_usd NUMERIC(20, 8),
    
    -- Pool information
    pool_1 VARCHAR(100),
    pool_2 VARCHAR(100),
    
    -- Swap characteristics
    is_streaming_swap BOOLEAN DEFAULT FALSE,
    swap_slip NUMERIC(10, 6),
    
    -- Volume classification
    volume_tier VARCHAR(20), -- '<=$100', '100-1000', etc.
    
    -- Raw data for debugging
    raw_data JSONB,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Primary key includes timestamp for TimescaleDB compatibility
    PRIMARY KEY (timestamp, tx_hash, source)
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('swaps', 'timestamp');

-- Create indexes (after hypertable creation)
CREATE INDEX idx_swaps_timestamp ON swaps (timestamp DESC);
CREATE INDEX idx_swaps_source ON swaps (source);
CREATE INDEX idx_swaps_user ON swaps (user_address);
CREATE INDEX idx_swaps_date ON swaps (date_only);
CREATE INDEX idx_swaps_volume_tier ON swaps (volume_tier);
CREATE INDEX idx_swaps_pools ON swaps (pool_1, pool_2);
CREATE INDEX idx_swaps_assets ON swaps (in_asset, out_asset);
CREATE INDEX idx_swaps_tx_hash ON swaps (tx_hash);

-- Table for tracking API sync status
CREATE TABLE sync_status (
    id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL UNIQUE,
    last_synced_timestamp TIMESTAMPTZ,
    last_synced_block BIGINT,
    next_page_token TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert initial sync status
INSERT INTO sync_status (source) VALUES 
('thorchain'), ('mayachain'), ('lifi'), ('1inch')
ON CONFLICT (source) DO NOTHING;

-- Materialized views for common aggregations
CREATE MATERIALIZED VIEW daily_metrics AS
SELECT 
    date_only,
    source,
    COUNT(*) as swap_count,
    COUNT(DISTINCT user_address) as unique_users,
    SUM(in_amount_usd) as total_volume_usd,
    SUM(total_fee_usd) as total_fees_usd,
    AVG(in_amount_usd) as avg_volume_per_swap
FROM swaps 
GROUP BY date_only, source
ORDER BY date_only DESC;

CREATE UNIQUE INDEX ON daily_metrics (date_only, source);

-- Pool metrics view
CREATE MATERIALIZED VIEW pool_metrics AS
SELECT 
    date_only,
    COALESCE(pool_1, pool_2) as pool_name,
    source,
    COUNT(*) as swap_count,
    SUM(in_amount_usd) as volume_usd,
    COUNT(DISTINCT user_address) as unique_users
FROM swaps 
WHERE pool_1 IS NOT NULL OR pool_2 IS NOT NULL
GROUP BY date_only, COALESCE(pool_1, pool_2), source
ORDER BY date_only DESC, volume_usd DESC;

CREATE UNIQUE INDEX ON pool_metrics (date_only, pool_name, source);

-- Volume tier metrics view
CREATE MATERIALIZED VIEW volume_tier_metrics AS
SELECT 
    date_only,
    volume_tier,
    source,
    COUNT(*) as swap_count,
    SUM(in_amount_usd) as total_volume_usd
FROM swaps 
GROUP BY date_only, volume_tier, source
ORDER BY date_only DESC;

CREATE UNIQUE INDEX ON volume_tier_metrics (date_only, volume_tier, source);

-- Platform metrics view
CREATE MATERIALIZED VIEW platform_metrics AS
SELECT 
    date_only,
    platform,
    source,
    COUNT(*) as swap_count,
    SUM(in_amount_usd) as total_volume_usd,
    SUM(total_fee_usd) as total_fees_usd
FROM swaps 
GROUP BY date_only, platform, source
ORDER BY date_only DESC;

CREATE UNIQUE INDEX ON platform_metrics (date_only, platform, source);

-- Create refresh function for all views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY daily_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY pool_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY volume_tier_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY platform_metrics;
END;
$$ LANGUAGE plpgsql;

-- Create function to get database stats
CREATE OR REPLACE FUNCTION get_database_stats()
RETURNS TABLE (
    table_name text,
    row_count bigint,
    size_pretty text
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'swaps'::text as table_name,
        (SELECT COUNT(*) FROM swaps) as row_count,
        pg_size_pretty(pg_total_relation_size('swaps')) as size_pretty
    UNION ALL
    SELECT 
        'sync_status'::text as table_name,
        (SELECT COUNT(*) FROM sync_status) as row_count,
        pg_size_pretty(pg_total_relation_size('sync_status')) as size_pretty;
END;
$$ LANGUAGE plpgsql;