-- Migration: send aggregator fee rows back through the router classifier
--
-- The classifier now demotes a row to 'other' when a fee receiver sent the
-- transaction itself. Such a row is the receiver's own swap through the
-- aggregator router: the tokens it received are swap output, not an
-- integrator fee, but the old tx.to-only check kept them.
--
-- Rows the classifier already verified are never re-checked, so clear their
-- verification tag. The next sync cycle re-checks each row (one
-- eth_getTransactionByHash call per row). Kept rows get 'router_check' back,
-- and the enricher keeps their existing fee value. Synthetic li.quest gap
-- rows (fee_data_source = 'lifi') are not touched.
--
-- Idempotent: safe to run more than once.

BEGIN;

UPDATE dex_aggregator_revenue
SET volume_data_source = NULL,
    updated_at = NOW()
WHERE fee_data_source = 'etherscan'
  AND protocol IN ('1inch', 'kyberswap')
  AND volume_data_source IN ('router_check', 'estimated');

COMMIT;
