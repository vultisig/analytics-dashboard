-- SwapKit affiliate fees accrued at the provider but not yet paid out.
-- One row per (snapshot, provider, platform, token). Near-Intents credits fees
-- to a per-app implicit account inside intents.near; the payout later lands
-- as a lump sum on the EVM fee wallet, which is why receipts alone cannot
-- split revenue by platform.

CREATE TABLE IF NOT EXISTS swapkit_accruals (
    snapshot_at TIMESTAMPTZ NOT NULL,
    provider VARCHAR(30) NOT NULL,
    platform VARCHAR(50) NOT NULL,
    token_id VARCHAR(160) NOT NULL,
    amount_raw NUMERIC(40, 0) NOT NULL,
    amount_usd NUMERIC(20, 8),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (snapshot_at, provider, platform, token_id)
);

CREATE INDEX IF NOT EXISTS idx_swapkit_accruals_latest
    ON swapkit_accruals (provider, platform, snapshot_at DESC);
