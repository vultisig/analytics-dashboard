"""Near-Intents volume. Missing JWT fail-opens; rows are volume-only."""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from config import config
from ingestors.base import BaseIngestor
from ingestors.swapkit_volume import (
    SwapKitVolumeDraft,
    build_swapkit_swap_record,
    parse_decimal,
)

logger = logging.getLogger(__name__)

NEAR_TOOL = "near-intents"
SYNC_SOURCE = "near-intents"
SUCCESS_STATUS = "SUCCESS"
PAGE_SIZE = 50
MAX_PAGES_PER_SYNC = 10
JWT_MISSING = "NEAR_INTENTS_JWT not set"


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


class NearIntentsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__(SYNC_SOURCE)
        self.api_url = allowlisted_near_url(config.NEAR_INTENTS_API_URL)
        self.affiliate = config.NEAR_INTENTS_AFFILIATE
        self.jwt = config.NEAR_INTENTS_JWT

    def fetch_data(self, **kwargs) -> Dict:
        return self.fetch_page(kwargs.get("next_page_token"))

    def fetch_page(self, cursor: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        if not self.jwt:
            raise RuntimeError(JWT_MISSING)
        params = {
            "affiliate": self.affiliate,
            "statuses": SUCCESS_STATUS,
            "numberOfTransactions": PAGE_SIZE,
            "direction": "next",
        }
        if cursor:
            if cursor.get("lastDepositAddress"):
                params["lastDepositAddress"] = cursor["lastDepositAddress"]
            if cursor.get("lastDepositMemo"):
                params["lastDepositMemo"] = cursor["lastDepositMemo"]
        host = urlparse(self.api_url).hostname or ""
        if host != "explorer.near-intents.org":
            raise ValueError("refusing Near-Intents host outside allowlist")
        headers = {"Authorization": f"Bearer {self.jwt}"}
        response = self.session.get(
            self.api_url,
            params=params,
            headers=headers,
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
        if not isinstance(raw_swap, dict):
            return None
        if raw_swap.get("status") != SUCCESS_STATUS:
            return None
        tx_hash = near_tx_hash(raw_swap)
        if not tx_hash:
            return None
        in_usd = parse_decimal(raw_swap.get("amountInUsd"))
        out_usd = parse_decimal(raw_swap.get("amountOutUsd"))
        draft = SwapKitVolumeDraft(
            tool=NEAR_TOOL,
            timestamp=parse_near_timestamp(raw_swap),
            tx_hash=tx_hash,
            user_address=near_user(raw_swap),
            in_asset=str(raw_swap.get("originAsset") or ""),
            out_asset=str(raw_swap.get("destinationAsset") or ""),
            in_amount=parse_decimal(raw_swap.get("amountInFormatted")),
            out_amount=parse_decimal(raw_swap.get("amountOutFormatted")),
            in_amount_usd=in_usd,
            out_amount_usd=out_usd,
            in_amount_raw=str(raw_swap.get("amountIn") or "0"),
            raw_data=raw_swap,
        )
        return build_swapkit_swap_record(draft)

    def _next_cursor(self, rows: List[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        if len(rows) < PAGE_SIZE:
            return None
        last = rows[-1]
        address = last.get("depositAddress")
        if not address:
            return None
        cursor = {"lastDepositAddress": str(address)}
        if last.get("depositMemo") is not None:
            cursor["lastDepositMemo"] = str(last.get("depositMemo"))
        return cursor

    def ingest(self) -> Dict[str, Any]:
        if not self.jwt:
            logger.warning("Skipping Near-Intents ingest: %s", JWT_MISSING)
            return _result(0, None, JWT_MISSING)
        inserted = 0
        latest_ts = None
        cursor = None
        try:
            for _ in range(MAX_PAGES_PER_SYNC):
                rows = self.fetch_page(cursor)
                records = [row for row in (self.parse_swap(n) for n in rows) if row]
                if records:
                    inserted += self._insert_swaps(records)
                    if latest_ts is None:
                        latest_ts = records[0].get("timestamp")
                cursor = self._next_cursor(rows)
                if not cursor:
                    break
            return _result(inserted, latest_ts, None)
        except Exception as exc:
            logger.error("Near-Intents ingest failed: %s", exc)
            return _result(inserted, latest_ts, str(exc))

    @staticmethod
    def _insert_swaps(records):
        from database.connection import db_manager
        return db_manager.insert_swaps(records)


def _result(inserted: int, latest_ts, error: Optional[str]) -> Dict[str, Any]:
    return {
        "source": SYNC_SOURCE,
        "inserted": inserted,
        "latest_ts": latest_ts,
        "error": error,
    }
