-- dex_aggregator_revenue.timestamp is TIMESTAMP (naive) while every other timestamp
-- column is TIMESTAMPTZ - including this table's own created_at/updated_at and
-- swaps.timestamp. All writers supply UTC values and API sessions are pinned to UTC
-- (database/connection.py), so the stored wall-clock is UTC and the USING clause
-- below is lossless.
--
-- After this is applied in prod, the read-side compensators can be deleted:
-- iso_utc()'s naive branch and get_sort_key_for_timestamp() in api_server.py
-- (tracked in issue #56).
--
-- Operational notes:
--   * Takes an ACCESS EXCLUSIVE lock and rewrites the table + its indexes.
--     Size check first:  SELECT pg_size_pretty(pg_total_relation_size('dex_aggregator_revenue'));
--     Run off-peak; the sync service should be idle during the rewrite.
--   * Not a hypertable (only swaps is), so this is a single plain ALTER.
--   * Run with:  python migrations/run_migration.py migrations/dex_aggregator_revenue_timestamptz.sql

ALTER TABLE dex_aggregator_revenue
    ALTER COLUMN "timestamp" TYPE TIMESTAMPTZ
    USING "timestamp" AT TIME ZONE 'UTC';
