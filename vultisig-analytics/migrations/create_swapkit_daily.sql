-- swapkit_daily: SwapKit-reported earned revenue and volume per UTC day and provider; filled by an external job.
-- Idempotent: safe to run more than once.

CREATE TABLE IF NOT EXISTS swapkit_daily (
    date          DATE          NOT NULL,
    provider      VARCHAR(32)   NOT NULL,
    revenue_usd   NUMERIC(20,6) NOT NULL,
    volume_usd    NUMERIC(24,6) NOT NULL,
    first_seen_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (date, provider)
);

-- Named checks that also exclude NaN (NaN passes ">= 0" in PostgreSQL). The ALTERs make a table that
-- an earlier version of this file created end up with the same checks. Safe to run again.
ALTER TABLE swapkit_daily DROP CONSTRAINT IF EXISTS swapkit_daily_revenue_ok;
ALTER TABLE swapkit_daily ADD CONSTRAINT swapkit_daily_revenue_ok
    CHECK (revenue_usd >= 0 AND revenue_usd <> 'NaN');
ALTER TABLE swapkit_daily DROP CONSTRAINT IF EXISTS swapkit_daily_volume_ok;
ALTER TABLE swapkit_daily ADD CONSTRAINT swapkit_daily_volume_ok
    CHECK (volume_usd >= 0 AND volume_usd <> 'NaN');

-- Status writer for the external job. The job's role gets EXECUTE on this function
-- and no direct rights on sync_status, so it can only touch the 'swapkit-earned' row.
-- Success (p_error IS NULL): p_latest must be a closed UTC day (before today), else an exception.
-- Failure: p_error must be a fixed code ^[A-Z_]{2,32}$, else 'INVALID_CODE' is stored. A failed call
-- keeps the previous success time and latest day. No secrets, no IDs.
CREATE OR REPLACE FUNCTION record_swapkit_sync(p_latest DATE, p_error TEXT DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_code TEXT;
BEGIN
    IF p_error IS NULL THEN
        IF p_latest IS NULL OR p_latest >= (now() AT TIME ZONE 'UTC')::date THEN
            RAISE EXCEPTION 'record_swapkit_sync: success needs a latest day before today (UTC)'
                USING ERRCODE = '22023';
        END IF;
    ELSE
        v_code := CASE WHEN p_error ~ '^[A-Z_]{2,32}$' THEN p_error ELSE 'INVALID_CODE' END;
    END IF;

    INSERT INTO sync_status (source, last_synced_timestamp, latest_data_timestamp,
                             error_count, last_error, is_active, updated_at)
    VALUES ('swapkit-earned',
            CASE WHEN p_error IS NULL THEN now() END,
            CASE WHEN p_error IS NULL THEN (p_latest::timestamp AT TIME ZONE 'UTC') END,
            CASE WHEN p_error IS NULL THEN 0 ELSE 1 END,
            v_code,
            TRUE,
            now())
    ON CONFLICT (source) DO UPDATE SET
        last_synced_timestamp = COALESCE(EXCLUDED.last_synced_timestamp, sync_status.last_synced_timestamp),
        latest_data_timestamp = COALESCE(EXCLUDED.latest_data_timestamp, sync_status.latest_data_timestamp),
        error_count = CASE WHEN p_error IS NULL THEN 0
                           ELSE LEAST(COALESCE(sync_status.error_count, 0), 2147483646) + 1 END,
        last_error  = v_code,
        is_active   = TRUE,
        updated_at  = now();
END;
$$;

REVOKE ALL ON FUNCTION record_swapkit_sync(DATE, TEXT) FROM PUBLIC;
