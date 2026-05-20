-- Per-(protocol, chain) cursor index for EtherscanIngestor.
--
-- EtherscanIngestor runs `SELECT MAX(block_number) FROM dex_aggregator_revenue
-- WHERE protocol = ? AND chain = ?` once per (source, chain) every sync —
-- that's 2 sources * 8 chains = 16 lookups every 15 minutes. The existing
-- composite index idx_dex_revenue_query(protocol, chain, timestamp) does NOT
-- cover block_number, so Postgres has to do a heap fetch for the MAX.
--
-- This index turns it into a 1-row index-only scan (DESC tail of the
-- ordered key), shaving the per-sync cursor lookups from ~ms each to
-- sub-ms.
CREATE INDEX IF NOT EXISTS idx_dex_revenue_cursor
ON dex_aggregator_revenue (protocol, chain, block_number DESC);
