"""Classify SwapKit fee-wallet receipts by the ERC-20 sender.

One dashboard bucket (`swapkit`). Fee vs payout is derived from
`from_address` — no extra column. Allowlist is exact lowercase match.
"""

from typing import Optional, Tuple

from config import config

OTHER_PROTOCOLS = frozenset({"other", "kyberswap"})
BACKFILL_SOURCE_PROTOCOLS = tuple(sorted(OTHER_PROTOCOLS))


def normalize_address(address: Optional[str]) -> str:
    return (address or "").lower()


def protocol_for_sender(from_address: Optional[str]) -> Optional[str]:
    addr = normalize_address(from_address)
    if addr in config.SWAPKIT_FEE_SENDERS or addr in config.SWAPKIT_PAYOUT_SENDERS:
        return config.SWAPKIT_PROTOCOL
    return None


def is_swapkit_fee_sender(from_address: Optional[str]) -> bool:
    return normalize_address(from_address) in config.SWAPKIT_FEE_SENDERS


def is_swapkit_payout_sender(from_address: Optional[str]) -> bool:
    return normalize_address(from_address) in config.SWAPKIT_PAYOUT_SENDERS


def all_swapkit_senders() -> Tuple[str, ...]:
    return tuple(sorted(config.SWAPKIT_FEE_SENDERS | config.SWAPKIT_PAYOUT_SENDERS))


def backfill_swapkit_rows(db) -> int:
    """Promote allowlisted `other`/`kyberswap` senders. Leaves 1inch alone."""
    senders = all_swapkit_senders()
    if not senders:
        return 0
    cur = db.cursor()
    try:
        cur.execute(
            """
            UPDATE dex_aggregator_revenue
            SET protocol = %s, updated_at = NOW()
            WHERE LOWER(from_address) IN %s
              AND protocol IN %s
            """,
            (config.SWAPKIT_PROTOCOL, senders, BACKFILL_SOURCE_PROTOCOLS),
        )
        return cur.rowcount
    finally:
        cur.close()


def plan_swapkit_enrichment(
    from_address: Optional[str],
    fee_usd: float,
    existing_fee_usd: float,
) -> Tuple[dict, str]:
    """Fee always priced; volume only for SKWrap. Stamp stops payout retries."""
    updates: dict = {}
    if fee_usd and not existing_fee_usd:
        updates["actual_fee_usd"] = fee_usd
    if fee_usd and is_swapkit_fee_sender(from_address):
        updates["swap_volume_usd"] = fee_usd * config.AFFILIATE_VOLUME_TO_FEE_MULTIPLIER
    stamp = "payout" if is_swapkit_payout_sender(from_address) else "estimated"
    return updates, stamp
