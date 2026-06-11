-- Migration: LI.FI tool attribution + enricher loop reset
--
-- 1. Record the executing tool (li.quest `tool` field) on every lifi swap.
-- 2. Credit 1inch-executed LI.FI swaps to 1inch:
--    - swaps side: relabel source so the existing `source != '1inch'`
--      filters hide them from the lifi series (no double count);
--    - dex_aggregator_revenue side: re-attribute their fee rows (including
--      ones the router classifier previously demoted to 'other') with the
--      exact volume/fee/path from li.quest.
-- 3. Reset etherscan rows the old enricher stamped 'estimated' while their
--    volume was still NULL, so the classifier re-verifies and the fixed
--    enricher prices them.
--
-- Idempotent: safe to run more than once.

BEGIN;

ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tool TEXT;

UPDATE swaps
SET tool = raw_data #>> '{bridge_metadata,tool}'
WHERE source = 'lifi'
  AND tool IS NULL
  AND raw_data IS NOT NULL;

UPDATE swaps
SET source = '1inch'
WHERE source = 'lifi'
  AND tool = '1inch';

UPDATE dex_aggregator_revenue d
SET protocol = s.tool,
    swap_volume_usd = s.in_amount_usd,
    actual_fee_usd = COALESCE(s.affiliate_fee_usd, 0),
    token_in_symbol = NULLIF(split_part(s.in_asset, '-', 1), ''),
    token_out_symbol = NULLIF(split_part(s.out_asset, '-', 1), ''),
    amount_in = s.in_amount,
    volume_data_source = 'router_check',
    updated_at = NOW()
FROM swaps s
WHERE LOWER(s.in_tx_id) = LOWER(d.tx_hash)
  AND s.tool = '1inch'
  AND d.fee_data_source = 'etherscan';

UPDATE dex_aggregator_revenue
SET volume_data_source = NULL
WHERE fee_data_source = 'etherscan'
  AND volume_data_source = 'estimated'
  AND swap_volume_usd IS NULL;

COMMIT;

SELECT refresh_materialized_views();
