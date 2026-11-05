#!/usr/bin/env python3
"""
One-shot backfill for THORChain and MayaChain affiliate swaps.

The parsers used to (a) skip swaps whose input was the chain's native asset
(THOR.RUNE, MAYA.CACAO), because those carry no affiliate out-leg,
(b) credit the first affiliate out-leg to us on referral swaps with two
affiliates, and (c) store streaming swaps while they were still pending.
Before the late-swap reconciliation in main.sync_source, a swap that
finished after the sync had moved past it was never picked up at all.

This job re-pages Midgard from now back to --since and upserts every swap
with the current parser: new rows are inserted, existing rows get every
parser-owned column refreshed (the `tool` column belongs to LI.FI and stays).

Usage (from vultisig-analytics/):
    python3 jobs/backfill_midgard_fees.py --source thorchain --dry-run
    python3 jobs/backfill_midgard_fees.py --source mayachain --since 2024-01-01
"""
import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    date_only = EXCLUDED.date_only,
    block_height = EXCLUDED.block_height,
    user_address = EXCLUDED.user_address,
    in_asset = EXCLUDED.in_asset,
    in_amount = EXCLUDED.in_amount,
    in_amount_usd = EXCLUDED.in_amount_usd,
    out_asset = EXCLUDED.out_asset,
    out_amount = EXCLUDED.out_amount,
    out_amount_usd = EXCLUDED.out_amount_usd,
    total_fee_usd = EXCLUDED.total_fee_usd,
    network_fee_usd = EXCLUDED.network_fee_usd,
    liquidity_fee_usd = EXCLUDED.liquidity_fee_usd,
    affiliate_fee_usd = EXCLUDED.affiliate_fee_usd,
    pool_1 = EXCLUDED.pool_1,
    pool_2 = EXCLUDED.pool_2,
    is_streaming_swap = EXCLUDED.is_streaming_swap,
    swap_slip = EXCLUDED.swap_slip,
    volume_tier = EXCLUDED.volume_tier,
    raw_data = EXCLUDED.raw_data,
    platform = EXCLUDED.platform,
    in_address = EXCLUDED.in_address,
    in_tx_id = EXCLUDED.in_tx_id,
    in_amount_raw = EXCLUDED.in_amount_raw,
    out_addresses = EXCLUDED.out_addresses,
    out_tx_ids = EXCLUDED.out_tx_ids,
    out_heights = EXCLUDED.out_heights,
    affiliate_addresses = EXCLUDED.affiliate_addresses,
    affiliate_fees_bps = EXCLUDED.affiliate_fees_bps,
    metadata_complete = EXCLUDED.metadata_complete,
    in_price_usd = EXCLUDED.in_price_usd,
    out_price_usd = EXCLUDED.out_price_usd,
    network_fees_raw = EXCLUDED.network_fees_raw,
    pools_used = EXCLUDED.pools_used,
    swap_status = EXCLUDED.swap_status,
    swap_type = EXCLUDED.swap_type,
    memo = EXCLUDED.memo
"""


def _make_ingestor(source: str):
    if source == 'thorchain':
        from ingestors.thorchain import THORChainIngestor
        return THORChainIngestor()
    from ingestors.mayachain import MayaChainIngestor
    return MayaChainIngestor()


def backfill(source: str, since: datetime, dry_run: bool, ingestor=None, db=None) -> dict:
    """Returns {'pages', 'fetched', 'written', 'fee_by_month'}."""
    ingestor = ingestor or _make_ingestor(source)
    if db is None and not dry_run:
        from database.connection import DatabaseManager
        db = DatabaseManager()

    next_token = ''
    pages = fetched = written = 0
    fee_by_month = defaultdict(float)
    while True:
        response = ingestor.fetch_data(next_page_token=next_token)
        actions = response.get('actions', [])
        if not actions:
            break

        records = []
        reached_since = False
        for action in actions:
            parsed = ingestor.parse_swap(action)
            if not parsed:
                continue
            if parsed['timestamp'] < since:
                reached_since = True
                continue
            parsed.setdefault('tool', None)
            records.append(parsed)
            fee_by_month[parsed['timestamp'].strftime('%Y-%m')] += float(parsed['affiliate_fee_usd'] or 0)
        fetched += len(actions)

        if records and not dry_run:
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(UPSERT_QUERY, records)
                    conn.commit()
            written += len(records)

        pages += 1
        logger.info(f"Page {pages}: {len(actions)} actions, {len(records)} parsed, "
                    f"{fetched} fetched / {written} upserted total")

        next_token = response.get('meta', {}).get('nextPageToken')
        if reached_since or not next_token:
            break

    for month in sorted(fee_by_month):
        logger.info(f"{source} {month}: affiliate_fee_usd {fee_by_month[month]:,.2f}")
    logger.info(f"Backfill complete: {fetched} actions fetched, {written} rows upserted, "
                f"affiliate_fee_usd total {sum(fee_by_month.values()):,.2f}"
                + (" (dry run, nothing written)" if dry_run else ""))
    return {'pages': pages, 'fetched': fetched, 'written': written, 'fee_by_month': dict(fee_by_month)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill THORChain/MayaChain swaps and affiliate fees')
    parser.add_argument('--source', choices=('thorchain', 'mayachain'), required=True)
    parser.add_argument('--since', default='2024-01-01',
                        help='Oldest swap date to backfill, YYYY-MM-DD (default 2024-01-01)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and parse without writing')
    args = parser.parse_args()
    since = datetime.strptime(args.since, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    backfill(args.source, since, args.dry_run)
