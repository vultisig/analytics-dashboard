"""Tx-level router-source classifier for dex_aggregator_revenue rows.

Etherscan's `tokentx` endpoint surfaces every ERC20 transfer to a fee
receiver, but only a subset of those transfers are actual aggregator-fee
deposits — the receiver address also catches treasury moves, OTC settlements,
cross-chain affiliate fees from THORChain/Maya routers (already captured
separately), and on-chain spam.

This classifier asks Etherscan one extra question per candidate row —
`eth_getTransactionByHash(tx_hash)` — and inspects the top-level `tx.to`.
If it matches a known router for the row's protocol, the row stays
classified as that aggregator. Transfers from the LI.FI Diamond get a second
look: when the matching li.quest swap was executed by an attributed
aggregator tool (config.ATTRIBUTED_LIFI_TOOLS), the row is credited to that
aggregator with the exact volume/fee from li.quest. Everything else is
demoted to `'other'` (already established for unattributed rows; analytics
queries filter to `protocol IN ARKHAM_PROVIDERS`).

Fail-open: on API errors we LEAVE THE ROW ALONE (don't touch protocol or
the verification tag) so the next classifier cycle retries. This avoids the
silent-failure pattern that hid Arkham's 402.

The `volume_data_source` column doubles as the verification tag — currently
NULL on every etherscan-sourced row, so setting it to `'router_check'`
marks the row as classifier-processed and avoids re-checking.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple

import psycopg2
import requests

from config import config
from .protocol_identifier import KNOWN_ROUTERS

logger = logging.getLogger(__name__)

ETHERSCAN_V2_URL = 'https://api.etherscan.io/v2/api'
PER_REQUEST_DELAY_SECONDS = 0.5
VERIFICATION_TAG = 'router_check'

# (display chain name → chainid) for the Etherscan V2 lookup. Mirrors
# etherscan_ingestor.DEFAULT_CHAINS, kept here to avoid a circular import.
CHAIN_TO_ID: Dict[str, int] = {
    'Ethereum': 1,
    'BSC': 56,
    'Polygon': 137,
    'Arbitrum': 42161,
    'Optimism': 10,
    'Base': 8453,
    'Avalanche': 43114,
    'Blast': 81457,
}


def _router_set(protocol: str) -> Set[str]:
    return {addr.lower() for addr in KNOWN_ROUTERS.get(protocol, [])}


def fetch_tx_to(
    api_key: str,
    chainid: int,
    tx_hash: str,
    session: Optional[requests.Session] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve tx.to via Etherscan V2 proxy/eth_getTransactionByHash.

    Returns (tx_to_lowercase, error_message). On success error is None.
    On any failure (HTTP, missing field, unexpected envelope) returns
    (None, error_string) so the caller can fail-open and skip the row.
    """
    sess = session or requests
    try:
        r = sess.get(
            ETHERSCAN_V2_URL,
            params={
                'chainid': chainid,
                'module': 'proxy',
                'action': 'eth_getTransactionByHash',
                'txhash': tx_hash,
                'apikey': api_key,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        return None, f"HTTP error: {e}"

    # Etherscan V2 proxy errors come back as {jsonrpc, id, error: {...}} or
    # as a body with 'message' set. Result missing or null means tx not found.
    if 'error' in data and data.get('error'):
        return None, f"Etherscan error: {data['error']}"
    result = data.get('result')
    if not isinstance(result, dict):
        return None, f"unexpected result shape: {data!r}"
    to_addr = result.get('to')
    if to_addr is None:
        # Contract creation tx — `to` is null. Definitely not a router call.
        return '', None
    return to_addr.lower(), None


def classify_row(
    api_key: str,
    chainid: int,
    tx_hash: str,
    expected_protocol: str,
    session: Optional[requests.Session] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Classify one row. Returns (final_protocol, tx_to, error).

    final_protocol = expected_protocol if tx.to is a known router for it;
                   = 'other' if tx.to is not a known router;
                   = None if classification couldn't run (API error).

    tx_to lets the caller recognise LiFi-Diamond transfers, which get a
    second chance at attribution via the li.quest swap record.

    Caller writes the new protocol + verification tag only when error is None.
    """
    tx_to, err = fetch_tx_to(api_key, chainid, tx_hash, session=session)
    if err is not None:
        return None, None, err

    expected_routers = _router_set(expected_protocol)
    if not expected_routers:
        # No router config for this source — can't classify; demote to other
        # so this isn't a permanently unclassified row.
        return 'other', tx_to, None

    if tx_to in expected_routers:
        return expected_protocol, tx_to, None
    return 'other', tx_to, None


def iter_unverified_rows(
    db,
    batch_size: int = 500,
) -> Iterable[Tuple[int, str, str, str, datetime]]:
    """Yield (id, tx_hash, chain, protocol, timestamp) for etherscan-sourced
    rows that are still tagged as a known aggregator but haven't been
    router-verified.
    """
    cur = db.cursor(name='unverified_rows_cursor')
    cur.itersize = batch_size
    cur.execute(
        """
        SELECT id, tx_hash, chain, protocol, timestamp
        FROM dex_aggregator_revenue
        WHERE fee_data_source = 'etherscan'
          AND volume_data_source IS NULL
          AND protocol IN ('1inch', 'kyberswap')
        ORDER BY id ASC
        """
    )
    try:
        for row in cur:
            yield row
    finally:
        cur.close()


def fetch_lifi_swap(db, tx_hash: str) -> Optional[Dict]:
    """The li.quest swap whose sending tx produced this fee transfer, if any."""
    cur = db.cursor()
    cur.execute(
        """
        SELECT tool, in_amount_usd, affiliate_fee_usd, in_asset, out_asset, in_amount
        FROM swaps
        WHERE LOWER(in_tx_id) = LOWER(%s)
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (tx_hash,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    keys = ('tool', 'in_amount_usd', 'affiliate_fee_usd', 'in_asset', 'out_asset', 'in_amount')
    return dict(zip(keys, row))


def apply_classification(db, row_id: int, new_protocol: str) -> None:
    """Persist one classifier decision."""
    cur = db.cursor()
    cur.execute(
        """
        UPDATE dex_aggregator_revenue
        SET protocol = %s,
            volume_data_source = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (new_protocol, VERIFICATION_TAG, row_id),
    )
    cur.close()


def apply_attribution(db, row_id: int, swap: Dict) -> None:
    """Credit a LiFi-routed fee row to its executing aggregator, copying the
    exact volume/fee/path from the li.quest record."""
    cur = db.cursor()
    cur.execute(
        """
        UPDATE dex_aggregator_revenue
        SET protocol = %s,
            swap_volume_usd = %s,
            actual_fee_usd = %s,
            token_in_symbol = %s,
            token_out_symbol = %s,
            amount_in = %s,
            volume_data_source = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            swap['tool'],
            swap['in_amount_usd'],
            swap['affiliate_fee_usd'] or 0,
            _asset_symbol(swap['in_asset']),
            _asset_symbol(swap['out_asset']),
            swap['in_amount'],
            VERIFICATION_TAG,
            row_id,
        ),
    )
    cur.close()


def _asset_symbol(asset: Optional[str]) -> Optional[str]:
    """'USDC-1' (li.quest asset format) -> 'USDC'."""
    if not asset:
        return None
    return asset.split('-', 1)[0] or None


def _resolve_lifi_diamond(db, row_id: int, tx_hash: str, row_ts: datetime, counts: Dict[str, int]) -> None:
    """Attribute a LiFi-Diamond fee transfer via its li.quest swap record.

    No matching swap yet → leave the row unverified so the next cycle retries
    (the lifi sync runs in parallel and may not have landed it), up to a grace
    window — after that it's noise, demote.
    """
    swap = fetch_lifi_swap(db, tx_hash)
    if swap and swap['tool'] in config.ATTRIBUTED_LIFI_TOOLS:
        apply_attribution(db, row_id, swap)
        counts['attributed'] += 1
    elif swap:
        apply_classification(db, row_id, 'other')
        counts['demoted'] += 1
    elif datetime.utcnow() - row_ts > timedelta(days=config.LIFI_MATCH_GRACE_DAYS):
        apply_classification(db, row_id, 'other')
        counts['demoted'] += 1
    else:
        counts['deferred'] += 1
        return
    db.commit()


def reclassify_all(
    api_key: str,
    db,
    delay: float = PER_REQUEST_DELAY_SECONDS,
    session: Optional[requests.Session] = None,
) -> Dict[str, int]:
    """Run classifier across every unverified row. Returns counts dict."""
    counts = {
        'kept': 0, 'demoted': 0, 'attributed': 0, 'deferred': 0,
        'skipped_error': 0, 'skipped_unknown_chain': 0,
    }

    rows = list(iter_unverified_rows(db))
    logger.info(f"Classifying {len(rows)} unverified rows")

    for row_id, tx_hash, chain, protocol, row_ts in rows:
        chainid = CHAIN_TO_ID.get(chain)
        if chainid is None:
            logger.warning(f"row {row_id}: unknown chain '{chain}', skipping")
            counts['skipped_unknown_chain'] += 1
            continue

        final, tx_to, err = classify_row(api_key, chainid, tx_hash, protocol, session=session)
        if err is not None:
            logger.warning(f"row {row_id} (tx {tx_hash[:14]}): {err}")
            counts['skipped_error'] += 1
            time.sleep(delay)
            continue

        if final == protocol:
            counts['kept'] += 1
            apply_classification(db, row_id, final)
            db.commit()
        elif tx_to == config.LIFI_DIAMOND_ADDRESS:
            _resolve_lifi_diamond(db, row_id, tx_hash, row_ts, counts)
        else:
            counts['demoted'] += 1
            apply_classification(db, row_id, final)
            db.commit()
        time.sleep(delay)

    logger.info(
        f"Classifier done: kept={counts['kept']} demoted={counts['demoted']} "
        f"attributed={counts['attributed']} deferred={counts['deferred']} "
        f"skipped_error={counts['skipped_error']} skipped_unknown_chain={counts['skipped_unknown_chain']}"
    )
    return counts


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    api_key = os.environ.get('ETHERSCAN_API_KEY')
    db_url = os.environ.get('DATABASE_URL')
    if not api_key or not db_url:
        raise SystemExit("ETHERSCAN_API_KEY and DATABASE_URL must be set")
    db = psycopg2.connect(db_url)
    try:
        with requests.Session() as session:
            reclassify_all(api_key, db, session=session)
    finally:
        db.close()


if __name__ == '__main__':
    main()
