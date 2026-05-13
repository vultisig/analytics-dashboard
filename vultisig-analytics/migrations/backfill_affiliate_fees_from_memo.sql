-- Backfill affiliate_fees_bps from memo when Midgard's affiliateFee string
-- collapsed a multi-affiliate fee list to a single value.
--
-- Symptom: affiliate_addresses has length N > 1, affiliate_fees_bps has
-- length M != N (typically 1, value 0), so revenue queries that read
-- affiliate_fees_bps[1] produce 0 for the position-1 referrer.
-- See /api/referrals leaderboard rows with positive referralCount but $0 revenue.
--
-- Memo shape (THORChain / MAYAChain swap):
--   SWAP_TYPE:ASSET:DEST:LIM:AFFILIATE_LIST:FEE_LIST[:DEX_AGG:...]
-- split_part(memo, ':', 6) extracts FEE_LIST.
--
-- Safety: only rewrites rows where the memo-derived bps list parses cleanly
-- to integers AND its length matches affiliate_addresses, so we never widen
-- or truncate the affiliate array.

BEGIN;

-- 1. Snapshot affected rows before the update for auditing.
CREATE TABLE IF NOT EXISTS swaps_affiliate_fees_backfill_audit (
    tx_hash             TEXT PRIMARY KEY,
    source              TEXT NOT NULL,
    memo                TEXT,
    affiliate_addresses TEXT[],
    old_fees_bps        INTEGER[],
    new_fees_bps        INTEGER[],
    backfilled_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

WITH candidates AS (
    SELECT
        tx_hash,
        source,
        memo,
        affiliate_addresses,
        affiliate_fees_bps AS old_fees_bps,
        (
            SELECT array_agg(f::int ORDER BY ord)
            FROM unnest(
                     string_to_array(split_part(memo, ':', 6), '/')
                 ) WITH ORDINALITY AS t(f, ord)
            WHERE f ~ '^\d+$'
        ) AS new_fees_bps
    FROM swaps
    WHERE source IN ('thorchain', 'mayachain')
      AND affiliate_addresses IS NOT NULL
      AND COALESCE(array_length(affiliate_addresses, 1), 0) > 1
      AND COALESCE(array_length(affiliate_addresses, 1), 0)
          <> COALESCE(array_length(affiliate_fees_bps, 1), 0)
      AND memo IS NOT NULL
      AND memo LIKE '%:%:%:%:%:%'  -- at least 5 colons => 6 segments
)
INSERT INTO swaps_affiliate_fees_backfill_audit
    (tx_hash, source, memo, affiliate_addresses, old_fees_bps, new_fees_bps)
SELECT
    tx_hash, source, memo, affiliate_addresses, old_fees_bps, new_fees_bps
FROM candidates
WHERE new_fees_bps IS NOT NULL
  AND array_length(new_fees_bps, 1) = array_length(affiliate_addresses, 1)
ON CONFLICT (tx_hash) DO NOTHING;

-- 2. Inspect before committing. Expect: rows where new_fees_bps[1] > 0 and
--    old_fees_bps[1] = 0 (or NULL) — these are the previously $0-revenue
--    referrer rows that will now produce real revenue.
SELECT
    source,
    COUNT(*)                                                   AS rows_to_update,
    COUNT(*) FILTER (WHERE COALESCE(old_fees_bps[1], 0) = 0
                       AND COALESCE(new_fees_bps[1], 0) > 0)    AS rows_unlocking_pos1_revenue,
    COUNT(DISTINCT UPPER(affiliate_addresses[1]))              AS distinct_referrers_affected
FROM swaps_affiliate_fees_backfill_audit
GROUP BY source;

-- 3. Apply the fix.
UPDATE swaps s
SET affiliate_fees_bps = a.new_fees_bps
FROM swaps_affiliate_fees_backfill_audit a
WHERE s.tx_hash = a.tx_hash
  AND s.source  = a.source
  AND s.affiliate_fees_bps IS DISTINCT FROM a.new_fees_bps;

-- 4. Post-update sanity: every backfilled row's array lengths now match.
DO $$
DECLARE
    bad_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO bad_count
    FROM swaps s
    JOIN swaps_affiliate_fees_backfill_audit a USING (tx_hash)
    WHERE COALESCE(array_length(s.affiliate_addresses, 1), 0)
       <> COALESCE(array_length(s.affiliate_fees_bps, 1), 0);
    IF bad_count > 0 THEN
        RAISE EXCEPTION 'Backfill left % rows with mismatched array lengths', bad_count;
    END IF;
END $$;

-- Review the audit table, then COMMIT or ROLLBACK manually:
-- COMMIT;
