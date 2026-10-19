-- Migration: credit fee-receiver rows by router, and re-price unpriced fees
--
-- 1. The classifier used to keep a row only when tx.to was a router of the
--    receiver's label. The apps use 0x8E24...ABb9 as both the 1inch referrer
--    and the KyberSwap fee receiver, so every 1inch fee paid there was
--    demoted to 'other'. Restore each demoted row's receiver label and clear
--    its verification tag: the next sync re-checks it and credits the
--    aggregator whose router tx.to names. Rows that are not router fees
--    (direct sends, THORChain or MAYAChain payouts, LI.FI Diamond rows of
--    other tools) are demoted again. One eth_getTransactionByHash per row.
--
-- 2. Fees in tokens outside the enricher's CoinGecko symbol map were stored
--    at 0 USD. The enricher now prices them by contract through DefiLlama.
--    Send those rows back to the enricher.
--
-- Receiver addresses are the config.py defaults. If ONEINCH_FEE_RECEIVER or
-- KYBER_FEE_RECEIVER is overridden in the environment, edit the VALUES list.
--
-- Run after migrations/reverify_receiver_own_swaps.sql. Idempotent.

BEGIN;

UPDATE dex_aggregator_revenue d
SET protocol = r.label,
    volume_data_source = NULL,
    updated_at = NOW()
FROM (VALUES
    ('0xa4a4f610e89488eb4ecc6c63069f241a54485269', '1inch'),
    ('0x8e247a480449c84a5fdd25974a8501f3efa4abb9', 'kyberswap')
) AS r(receiver, label)
WHERE d.fee_data_source = 'etherscan'
  AND d.protocol = 'other'
  AND d.volume_data_source = 'router_check'
  AND LOWER(d.to_address) = r.receiver;

UPDATE dex_aggregator_revenue
SET swap_volume_usd = NULL,
    volume_data_source = 'router_check',
    updated_at = NOW()
WHERE fee_data_source = 'etherscan'
  AND protocol IN ('1inch', 'kyberswap')
  AND volume_data_source = 'estimated'
  AND COALESCE(actual_fee_usd, 0) = 0;

COMMIT;
