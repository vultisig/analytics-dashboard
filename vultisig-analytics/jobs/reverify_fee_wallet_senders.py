#!/usr/bin/env python3
"""
One-shot re-verification of every provider-tagged fee-wallet row.

The router classifier only ever sees rows the Etherscan ingestor wrote and
stamped NULL. Rows from the Arkham era, and rows the enricher stamped before
the classifier existed, kept whatever protocol the receiver implied — the
main fee wallet is the KyberSwap receiver, so THORChain / LI.FI settlement
into it has been booked as `kyberswap` (analytics-dashboard #16).

Two passes:
  1. Sender ownership (no API): SwapKit senders -> `swapkit`, Midgard-owned
     senders (THORChain router, Asgard vault) -> `other`.
  2. Router check (Etherscan tx.to) over every 1inch/kyberswap row the
     classifier has never verified, whatever ingestor wrote it.

Usage (from vultisig-analytics/):
    python3 jobs/reverify_fee_wallet_senders.py            # dry run: plan + counts
    python3 jobs/reverify_fee_wallet_senders.py --apply    # write both passes
"""
import argparse
import logging
import os
import sys
from typing import Dict, Iterable, List, Sequence, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestors.router_source_classifier import (  # noqa: E402
    iter_never_router_checked_rows,
    reclassify_all,
)
from ingestors.swapkit_senders import (  # noqa: E402
    OTHER_PROTOCOL,
    UNPRICED_VOLUME_STAMP,
    protocol_for_sender,
)

logger = logging.getLogger(__name__)

REVENUE_PROTOCOLS = ("1inch", "kyberswap")
SENDER_OWNER_STAMP = "sender_owner"

TOTALS_QUERY = """
    SELECT protocol, COUNT(*) AS rows, COALESCE(SUM(actual_fee_usd), 0) AS fee_usd
    FROM dex_aggregator_revenue
    GROUP BY protocol
    ORDER BY fee_usd DESC
"""

SENDER_ROWS_QUERY = """
    SELECT id, LOWER(from_address) AS from_address, protocol
    FROM dex_aggregator_revenue
    WHERE protocol IN %s AND from_address IS NOT NULL
"""

APPLY_OWNER_QUERY = """
    UPDATE dex_aggregator_revenue
    SET protocol = %s, volume_data_source = %s, updated_at = NOW()
    WHERE id = ANY(%s)
"""

# A row enriched as kyberswap carries a fee*200 volume guess; SwapKit never derives volume.
APPLY_SWAPKIT_OWNER_QUERY = """
    UPDATE dex_aggregator_revenue
    SET protocol = %s,
        swap_volume_usd = NULL,
        volume_data_source = CASE WHEN volume_data_source = 'estimated' THEN %s
                                  ELSE volume_data_source END,
        updated_at = NOW()
    WHERE id = ANY(%s)
"""

SenderRow = Tuple[int, str, str]


def plan_sender_ownership(rows: Iterable[SenderRow]) -> Dict[str, List[int]]:
    """Row ids to move, keyed by their owner-derived protocol."""
    moves: Dict[str, List[int]] = {}
    for row_id, from_address, protocol in rows:
        owner = protocol_for_sender(from_address)
        if owner and owner != protocol:
            moves.setdefault(owner, []).append(row_id)
    return moves


def fetch_totals(db) -> List[Tuple[str, int, float]]:
    cur = db.cursor()
    cur.execute(TOTALS_QUERY)
    try:
        return [(p, int(n), float(usd)) for p, n, usd in cur.fetchall()]
    finally:
        cur.close()


def fetch_sender_rows(db) -> List[SenderRow]:
    cur = db.cursor()
    cur.execute(SENDER_ROWS_QUERY, (REVENUE_PROTOCOLS,))
    try:
        return [(int(r[0]), r[1], r[2]) for r in cur.fetchall()]
    finally:
        cur.close()


def apply_sender_ownership(db, moves: Dict[str, List[int]]) -> int:
    cur = db.cursor()
    moved = 0
    try:
        for owner, ids in moves.items():
            if owner == OTHER_PROTOCOL:
                cur.execute(APPLY_OWNER_QUERY, (owner, SENDER_OWNER_STAMP, ids))
            else:
                cur.execute(APPLY_SWAPKIT_OWNER_QUERY, (owner, UNPRICED_VOLUME_STAMP, ids))
            moved += cur.rowcount
        db.commit()
    finally:
        cur.close()
    return moved


def print_totals(label: str, totals: Sequence[Tuple[str, int, float]]) -> None:
    print(f"--- {label} ---")
    for protocol, rows, fee_usd in totals:
        print(f"{protocol:>12}  rows={rows:>6}  fee_usd={fee_usd:>12,.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write changes (default is a dry run)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    import psycopg2
    import requests
    from dotenv import load_dotenv

    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    api_key = os.environ.get("ETHERSCAN_API_KEY")
    if not db_url or not api_key:
        raise SystemExit("DATABASE_URL and ETHERSCAN_API_KEY must be set")

    db = psycopg2.connect(db_url)
    try:
        print_totals("before", fetch_totals(db))
        moves = plan_sender_ownership(fetch_sender_rows(db))
        for owner, ids in moves.items():
            print(f"sender ownership: {len(ids)} rows -> {owner}")
        never_checked = list(iter_never_router_checked_rows(db))
        print(f"router check: {len(never_checked)} rows never verified by tx.to")
        if not args.apply:
            print("dry run — rerun with --apply to write")
            return
        moved = apply_sender_ownership(db, moves)
        print(f"sender ownership applied: {moved} rows")
        moved_ids = {i for ids in moves.values() for i in ids}
        remaining = [row for row in never_checked if row[0] not in moved_ids]
        with requests.Session() as session:
            counts = reclassify_all(api_key, db, session=session, rows=remaining)
        print(f"router check applied: {counts}")
        print_totals("after", fetch_totals(db))
    finally:
        db.close()


if __name__ == "__main__":
    main()
