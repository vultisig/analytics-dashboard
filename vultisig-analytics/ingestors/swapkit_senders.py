"""Classify fee-wallet receipts by the ERC-20 sender.

The main fee wallet collects from several integrations at once. SwapKit
senders get the `swapkit` bucket; senders owned by the Midgard ingestors
(THORChain router, Asgard vault) are booked `other` so THOR/Maya settlement
is never counted twice. Fee vs payout is derived from `from_address`.
Allowlist is exact lowercase match.
"""

from typing import Optional, Tuple

from config import config

OTHER_PROTOCOL = "other"
OTHER_PROTOCOLS = frozenset({OTHER_PROTOCOL, "kyberswap"})
BACKFILL_SOURCE_PROTOCOLS = tuple(sorted(OTHER_PROTOCOLS))


def normalize_address(address: Optional[str]) -> str:
    return (address or "").lower()


def protocol_for_sender(from_address: Optional[str]) -> Optional[str]:
    addr = normalize_address(from_address)
    if addr in config.SWAPKIT_FEE_SENDERS or addr in config.SWAPKIT_PAYOUT_SENDERS:
        return config.SWAPKIT_PROTOCOL
    if addr in config.MIDGARD_OWNED_FEE_WALLET_SENDERS:
        return OTHER_PROTOCOL
    return None


def is_swapkit_fee_sender(from_address: Optional[str]) -> bool:
    return normalize_address(from_address) in config.SWAPKIT_FEE_SENDERS


def is_swapkit_payout_sender(from_address: Optional[str]) -> bool:
    return normalize_address(from_address) in config.SWAPKIT_PAYOUT_SENDERS


def all_swapkit_senders() -> Tuple[str, ...]:
    return tuple(sorted(config.SWAPKIT_FEE_SENDERS | config.SWAPKIT_PAYOUT_SENDERS))


PROMOTE_TO_SWAPKIT_QUERY = """
    UPDATE dex_aggregator_revenue
    SET protocol = %s,
        swap_volume_usd = NULL,
        volume_data_source = CASE WHEN volume_data_source = 'estimated' THEN %s
                                  ELSE volume_data_source END,
        updated_at = NOW()
    WHERE LOWER(from_address) IN %s
      AND protocol IN %s
"""


def backfill_swapkit_rows(db) -> int:
    """Promote allowlisted `other`/`kyberswap` senders. Leaves 1inch alone.

    A row enriched while tagged kyberswap carries a fee*200 volume guess;
    promotion drops it. Already-priced rows are restamped so the enricher
    does not revisit them; unpriced ones keep their stamp and get priced.
    """
    senders = all_swapkit_senders()
    if not senders:
        return 0
    cur = db.cursor()
    try:
        cur.execute(
            PROMOTE_TO_SWAPKIT_QUERY,
            (config.SWAPKIT_PROTOCOL, UNPRICED_VOLUME_STAMP, senders, BACKFILL_SOURCE_PROTOCOLS),
        )
        return cur.rowcount
    finally:
        cur.close()


UNPRICED_VOLUME_STAMP = "unpriced"
PAYOUT_STAMP = "payout"


def plan_swapkit_enrichment(
    from_address: Optional[str],
    fee_usd: float,
    existing_fee_usd: float,
) -> Tuple[dict, str]:
    """Fee always priced; volume never derived.

    SwapKit affiliate rates vary per provider and per swap (30-65 bps seen
    on-chain), so `fee / rate` would be a guess. SKWrap rows keep their
    count with unknown volume. The stamp stops retries.
    """
    updates: dict = {}
    if fee_usd and not existing_fee_usd:
        updates["actual_fee_usd"] = fee_usd
    stamp = PAYOUT_STAMP if is_swapkit_payout_sender(from_address) else UNPRICED_VOLUME_STAMP
    return updates, stamp
