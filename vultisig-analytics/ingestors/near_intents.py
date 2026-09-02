"""Near-Intents volume from the partner explorer API. Missing JWT fail-opens.

Rows are volume-only. The affiliate the explorer is asked about is
`vultisigswapkit.near`; fees actually accrue to per-app implicit accounts
inside `intents.near` (see NEAR_INTENTS_APP_ACCOUNTS), so whether this
query isolates our swaps is unverified until a JWT reaches prod.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from config import config
from ingestors.base import BaseIngestor
from ingestors.paged_volume_sync import Page, ingest_feeds
from ingestors.swapkit_volume import (
    SwapKitVolumeDraft,
    build_swapkit_swap_record,
    parse_decimal,
)

logger = logging.getLogger(__name__)

NEAR_TOOL = "near-intents"
SYNC_SOURCE = "near-intents"
FEED_KEY = "explorer"
SUCCESS_STATUS = "SUCCESS"
PAGE_SIZE = 50
JWT_MISSING = "NEAR_INTENTS_JWT not set"
CURSOR_SEPARATOR = "|"


def allowlisted_near_url(url: str) -> str:
    allowed = frozenset({config.NEAR_INTENTS_API_URL})
    if url not in allowed:
        raise ValueError("refusing non-allowlisted Near-Intents URL")
    return url


def parse_near_timestamp(raw: Dict[str, Any]) -> datetime:
    created = raw.get("createdAt")
    if created:
        try:
            return datetime.fromisoformat(str(created).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            pass
    unix = raw.get("createdAtTimestamp")
    if unix:
        try:
            return datetime.fromtimestamp(int(unix), timezone.utc)
        except (TypeError, ValueError, OSError):
            pass
    return datetime.now(timezone.utc)


def first_hash(values: Any) -> Optional[str]:
    if isinstance(values, str) and values:
        return values
    if isinstance(values, list):
        for item in values:
            if item:
                return str(item)
    return None


def near_tx_hash(raw: Dict[str, Any]) -> Optional[str]:
    return (
        first_hash(raw.get("originChainTxHashes"))
        or first_hash(raw.get("intentHashes"))
        or first_hash(raw.get("nearTxHashes"))
        or first_hash(raw.get("destinationChainTxHashes"))
    )


def near_user(raw: Dict[str, Any]) -> str:
    senders = raw.get("senders") or []
    if senders:
        return str(senders[0])
    return str(raw.get("refundTo") or raw.get("recipient") or "")


def assert_newest_first(rows: List[Dict[str, Any]]) -> None:
    """The steady-state walk restarts at page 1 and assumes it is the newest.

    Unverified against a live JWT response; an oldest-first feed would
    silently stop ingesting after backfill, so refuse it out loud.
    """
    stamps = [parse_near_timestamp(r) for r in rows if isinstance(r, dict) and r.get("createdAt")]
    if len(stamps) >= 2 and stamps[0] < stamps[-1]:
        raise RuntimeError("Near-Intents explorer paged oldest-first; steady-state walk unsupported")


def encode_cursor(row: Dict[str, Any]) -> str:
    """The explorer pages on the last row's deposit address (+ memo)."""
    address = row.get("depositAddress")
    if not address:
        raise RuntimeError("Near-Intents page ended on a row without depositAddress; cannot paginate")
    memo = row.get("depositMemo")
    return f"{address}{CURSOR_SEPARATOR}{memo}" if memo is not None else str(address)


def cursor_params(cursor: Optional[str]) -> Dict[str, str]:
    if not cursor:
        return {}
    address, _, memo = cursor.partition(CURSOR_SEPARATOR)
    params = {"lastDepositAddress": address}
    if memo:
        params["lastDepositMemo"] = memo
    return params


class NearIntentsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__(SYNC_SOURCE)
        self.api_url = allowlisted_near_url(config.NEAR_INTENTS_API_URL)
        self.affiliate = config.NEAR_INTENTS_AFFILIATE
        self.jwt = config.NEAR_INTENTS_JWT

    def fetch_data(self, **kwargs) -> Dict:
        return {"rows": self.fetch_page(kwargs.get("next_page_token"))}

    def fetch_page(self, cursor: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.jwt:
            raise RuntimeError(JWT_MISSING)
        host = urlparse(self.api_url).hostname or ""
        if host != "explorer.near-intents.org":
            raise ValueError("refusing Near-Intents host outside allowlist")
        params = {
            "affiliate": self.affiliate,
            "statuses": SUCCESS_STATUS,
            "numberOfTransactions": PAGE_SIZE,
            "direction": "next",
            **cursor_params(cursor),
        }
        response = self.session.get(
            self.api_url,
            params=params,
            headers={"Authorization": f"Bearer {self.jwt}"},
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        delay = config.API_DELAYS.get(self.source_name, 0)
        if delay:
            time.sleep(delay)
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError("Near-Intents response was not a list")
        return payload

    def parse_swap(self, raw_swap: Dict) -> Optional[Dict]:
        if not isinstance(raw_swap, dict) or raw_swap.get("status") != SUCCESS_STATUS:
            return None
        tx_hash = near_tx_hash(raw_swap)
        if not tx_hash:
            return None
        draft = SwapKitVolumeDraft(
            tool=NEAR_TOOL,
            timestamp=parse_near_timestamp(raw_swap),
            tx_hash=tx_hash,
            user_address=near_user(raw_swap),
            in_asset=str(raw_swap.get("originAsset") or ""),
            out_asset=str(raw_swap.get("destinationAsset") or ""),
            in_amount=parse_decimal(raw_swap.get("amountInFormatted")),
            out_amount=parse_decimal(raw_swap.get("amountOutFormatted")),
            in_amount_usd=parse_decimal(raw_swap.get("amountInUsd")),
            out_amount_usd=parse_decimal(raw_swap.get("amountOutUsd")),
            in_amount_raw=str(raw_swap.get("amountIn") or "0"),
            raw_data=raw_swap,
        )
        return build_swapkit_swap_record(draft)

    def parsed_page(self, cursor: Optional[str]) -> Page:
        rows = self.fetch_page(cursor)
        assert_newest_first(rows)
        records = [row for row in (self.parse_swap(n) for n in rows) if row]
        next_cursor = encode_cursor(rows[-1]) if len(rows) >= PAGE_SIZE else None
        return records, next_cursor

    def ingest(self, state_token: Optional[str] = None) -> Dict[str, Any]:
        if not self.jwt:
            logger.warning("Skipping Near-Intents ingest: %s", JWT_MISSING)
            return {
                "source": SYNC_SOURCE,
                "inserted": 0,
                "pages": 0,
                "latest_ts": None,
                "error": JWT_MISSING,
                "next_state": state_token,
            }
        return ingest_feeds(SYNC_SOURCE, [(FEED_KEY, self.parsed_page)], state_token, self._insert_swaps)

    @staticmethod
    def _insert_swaps(records: List[Dict[str, Any]]) -> int:
        from database.connection import db_manager
        return db_manager.insert_swaps(records)
