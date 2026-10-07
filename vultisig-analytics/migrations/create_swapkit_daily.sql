-- swapkit_daily: SwapKit-reported earned revenue and volume per UTC day and provider; filled by an external job.
-- Idempotent: safe to run more than once.

CREATE TABLE IF NOT EXISTS swapkit_daily (
    date          DATE          NOT NULL,
    provider      VARCHAR(32)   NOT NULL,
    revenue_usd   NUMERIC(20,6) NOT NULL CHECK (revenue_usd >= 0),
    volume_usd    NUMERIC(24,6) NOT NULL CHECK (volume_usd >= 0),
    first_seen_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (date, provider)
);

-- Status writer for the external job. The job's role gets EXECUTE on this function
-- and no direct rights on sync_status, so it can only touch the 'swapkit-earned' row.
-- p_latest: last closed UTC day written. Used only on success; a failed call keeps the previous value.
-- p_error:  NULL on success; a short fixed error code on failure (no secrets, no IDs).
CREATE OR REPLACE FUNCTION record_swapkit_sync(p_latest DATE, p_error TEXT DEFAULT NULL)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
    INSERT INTO sync_status (source, last_synced_timestamp, latest_data_timestamp,
                             error_count, last_error, is_active, updated_at)
    VALUES ('swapkit-earned',
            CASE WHEN p_error IS NULL THEN now() END,
            CASE WHEN p_error IS NULL THEN (p_latest::timestamp AT TIME ZONE 'UTC') END,
            CASE WHEN p_error IS NULL THEN 0 ELSE 1 END,
            left(p_error, 64),
            TRUE,
            now())
    ON CONFLICT (source) DO UPDATE SET
        last_synced_timestamp = COALESCE(EXCLUDED.last_synced_timestamp, sync_status.last_synced_timestamp),
        latest_data_timestamp = COALESCE(EXCLUDED.latest_data_timestamp, sync_status.latest_data_timestamp),
        error_count = CASE WHEN p_error IS NULL THEN 0 ELSE sync_status.error_count + 1 END,
        last_error  = left(p_error, 64),
        is_active   = TRUE,
        updated_at  = now();
$$;

REVOKE ALL ON FUNCTION record_swapkit_sync(DATE, TEXT) FROM PUBLIC;
