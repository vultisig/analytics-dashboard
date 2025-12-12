-- Create tables for VULT token holder tier tracking
-- Used for displaying holder tier distribution and allowing users to check their tier

-- Table 1: Individual holder data
CREATE TABLE IF NOT EXISTS vult_holders (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL UNIQUE,
    vult_balance NUMERIC(38,18) NOT NULL,
    has_thorguard BOOLEAN DEFAULT FALSE,
    base_tier VARCHAR(20) NOT NULL,
    effective_tier VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for holder lookups
CREATE INDEX IF NOT EXISTS idx_vult_holders_address ON vult_holders(LOWER(address));
CREATE INDEX IF NOT EXISTS idx_vult_holders_effective_tier ON vult_holders(effective_tier);
CREATE INDEX IF NOT EXISTS idx_vult_holders_balance ON vult_holders(vult_balance DESC);

-- Table 2: Blacklist for addresses excluded from calculations (treasury, pools, exchanges)
CREATE TABLE IF NOT EXISTS vult_holders_blacklist (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vult_blacklist_address ON vult_holders_blacklist(LOWER(address));

-- Table 3: Aggregated tier statistics (cached for performance)
CREATE TABLE IF NOT EXISTS vult_tier_stats (
    id SERIAL PRIMARY KEY,
    tier VARCHAR(20) NOT NULL UNIQUE,
    holder_count INTEGER NOT NULL DEFAULT 0,
    total_vult_balance NUMERIC(38,18) NOT NULL DEFAULT 0,
    avg_vult_balance NUMERIC(38,18) NOT NULL DEFAULT 0,
    thorguard_boosted_count INTEGER DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Initialize tier stats with all tiers
INSERT INTO vult_tier_stats (tier, holder_count, total_vult_balance, avg_vult_balance, thorguard_boosted_count)
VALUES
    ('None', 0, 0, 0, 0),
    ('Bronze', 0, 0, 0, 0),
    ('Silver', 0, 0, 0, 0),
    ('Gold', 0, 0, 0, 0),
    ('Platinum', 0, 0, 0, 0),
    ('Diamond', 0, 0, 0, 0),
    ('Ultimate', 0, 0, 0, 0)
ON CONFLICT (tier) DO NOTHING;

-- Table 4: Metadata for tracking last update and totals
CREATE TABLE IF NOT EXISTS vult_holders_metadata (
    id SERIAL PRIMARY KEY,
    key VARCHAR(50) NOT NULL UNIQUE,
    value TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Initialize metadata
INSERT INTO vult_holders_metadata (key, value)
VALUES
    ('last_updated', '1970-01-01T00:00:00Z'),
    ('total_holders', '0'),
    ('total_supply_held', '0'),
    ('thorguard_holders', '0')
ON CONFLICT (key) DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE vult_holders IS 'VULT token holders with their balances and tier information';
COMMENT ON TABLE vult_holders_blacklist IS 'Addresses excluded from holder statistics (treasury, LP pools, exchanges)';
COMMENT ON TABLE vult_tier_stats IS 'Aggregated statistics per tier (cached, updated daily)';
COMMENT ON TABLE vult_holders_metadata IS 'Metadata about the holders data including last update time';

COMMENT ON COLUMN vult_holders.base_tier IS 'Tier based on VULT balance alone';
COMMENT ON COLUMN vult_holders.effective_tier IS 'Final tier after THORGuard NFT boost (max boost to Platinum)';
COMMENT ON COLUMN vult_tier_stats.thorguard_boosted_count IS 'Number of holders in this tier who were boosted by THORGuard NFT';
