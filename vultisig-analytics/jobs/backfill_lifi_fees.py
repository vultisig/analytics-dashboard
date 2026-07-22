#!/usr/bin/env python3
"""
One-shot backfill for LI.FI swaps.

The incremental sync stops at the newest known tx_hash, so it can never
pick up (a) historical vultisig-0 swaps that predate the integrator being
added to the sync list, or (b) corrected affiliate_fee_usd values on rows
ingested while the fee parser read includedSteps deltas instead of
feeCosts[].feeSplit.integratorFee.

This job re-pages the LI.FI analytics API from --days ago to now and
upserts: new rows are inserted, existing rows get their fee fields and
platform refreshed.

Usage (from vultisig-analytics/):
    python3 jobs/backfill_lifi_fees.py --days 365
    python3 jobs/backfill_lifi_fees.py --days 90 --dry-run
"""
import argparse
import logging
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SECONDS_PER_DAY = 86400

UPSERT_QUERY = """
INSERT INTO swaps (
    timestamp, tx_hash, source, date_only, block_height, user_address,
    in_asset, in_amount, in_amount_usd, out_asset, out_amount, out_amount_usd,
    total_fee_usd, network_fee_usd, liquidity_fee_usd, affiliate_fee_usd,
    pool_1, pool_2, is_streaming_swap, swap_slip, volume_tier, raw_data, platform,
    in_address, in_tx_id, in_amount_raw, out_addresses, out_tx_ids, out_heights,
    affiliate_addresses, affiliate_fees_bps, metadata_complete,
    in_price_usd, out_price_usd, network_fees_raw, pools_used, swap_status, swap_type, memo, tool
) VALUES (
    %(timestamp)s, %(tx_hash)s, %(source)s, %(date_only)s, %(block_height)s, %(user_address)s,
    %(in_asset)s, %(in_amount)s, %(in_amount_usd)s, %(out_asset)s, %(out_amount)s, %(out_amount_usd)s,
    %(total_fee_usd)s, %(network_fee_usd)s, %(liquidity_fee_usd)s, %(affiliate_fee_usd)s,
    %(pool_1)s, %(pool_2)s, %(is_streaming_swap)s, %(swap_slip)s, %(volume_tier)s, %(raw_data)s, %(platform)s,
    %(in_address)s, %(in_tx_id)s, %(in_amount_raw)s, %(out_addresses)s, %(out_tx_ids)s, %(out_heights)s,
    %(affiliate_addresses)s, %(affiliate_fees_bps)s, %(metadata_complete)s,
    %(in_price_usd)s, %(out_price_usd)s, %(network_fees_raw)s, %(pools_used)s, %(swap_status)s, %(swap_type)s, %(memo)s, %(tool)s
) ON CONFLICT (timestamp, tx_hash, source) DO UPDATE SET
    affiliate_fee_usd = EXCLUDED.affiliate_fee_usd,
    liquidity_fee_usd = EXCLUDED.liquidity_fee_usd,
    total_fee_usd = EXCLUDED.total_fee_usd,
    platform = EXCLUDED.platform
"""


def backfill(days: int, dry_run: bool) -> None:
    from ingestors.lifi import LiFiIngestor
    from database.connection import DatabaseManager

    ingestor = LiFiIngestor()
    db = DatabaseManager() if not dry_run else None
    from_timestamp = int(time.time()) - days * SECONDS_PER_DAY

    next_token = None
    pages = fetched = written = 0
    while True:
        response = ingestor.fetch_data(next_page_token=next_token, from_timestamp=from_timestamp)
        transfers = response.get('data', [])
        if not transfers:
            break

        records = []
        for transfer in transfers:
            parsed = ingestor.parse_swap(transfer)
            if parsed:
                parsed.setdefault('tool', None)
                records.append(parsed)
        fetched += len(transfers)

        if records and not dry_run:
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(UPSERT_QUERY, records)
                    conn.commit()
            written += len(records)

        pages += 1
        logger.info(f"Page {pages}: {len(transfers)} transfers, {len(records)} parsed, "
                    f"{fetched} fetched / {written} upserted total")

        next_token = response.get('next') if response.get('hasNext') else None
        if not next_token:
            break

    logger.info(f"Backfill complete: {fetched} transfers fetched, {written} rows upserted"
                + (" (dry run — nothing written)" if dry_run else ""))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill LI.FI swaps + fee fields')
    parser.add_argument('--days', type=int, default=365, help='How far back to backfill (default 365)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and parse without writing')
    args = parser.parse_args()
    backfill(args.days, args.dry_run)
