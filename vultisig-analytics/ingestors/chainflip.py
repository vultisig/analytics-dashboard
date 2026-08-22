"""Chainflip volume for the SwapKit affiliate (not the submitting broker)."""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import config
from ingestors.base import BaseIngestor
from ingestors.swapkit_volume import (
    SwapKitVolumeDraft,
    build_swapkit_swap_record,
    parse_decimal,
)

logger = logging.getLogger(__name__)

CHAINFLIP_TOOL = "chainflip"
SYNC_SOURCE = "chainflip"
PAGE_SIZE = 50
MAX_PAGES_PER_SYNC = 10
DEFAULT_ASSET_DECIMALS = 18
ASSET_DECIMALS = {
    "Usdc": 6,
    "Usdt": 6,
    "Btc": 8,
    "Sol": 9,
    "Dot": 10,
    "HubDot": 10,
}

BROKER_SWAPS_QUERY = """
query BrokerAffiliateSwaps($idSs58: String!, $first: Int!, $after: Cursor) {
  account: accountByIdSs58(idSs58: $idSs58) {
    swaps: swapRequestBeneficiariesByAccountId(
      first: $first
      after: $after
      orderBy: ID_DESC
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        type
        swapRequest: swapRequestBySwapRequestId {
          nativeId
          sourceAsset
          destinationAsset
          sourceChain
          destinationChain
          sourceAddress
          destinationAddress
          requestedBlockTimestamp
          completedBlockTimestamp
          depositAmount
          depositValueUsd
          transactionRefsBySwapRequestId { nodes { ref } }
          swapsBySwapRequestId(first: 20) {
            nodes { swapInputValueUsd swapOutputValueUsd }
          }
        }
      }
    }
  }
}
"""


def allowlisted_graphql_url(url: str) -> str:
    allowed = frozenset({config.CHAINFLIP_GRAPHQL_URL})
    if url not in allowed:
        raise ValueError("refusing non-allowlisted Chainflip GraphQL URL")
    return url


def amount_from_raw(raw: Any, asset: str) -> float:
    raw_int = parse_decimal(raw, 0.0)
    decimals = ASSET_DECIMALS.get(asset, DEFAULT_ASSET_DECIMALS)
    return raw_int / (10 ** decimals)


def first_tx_ref(swap_request: Dict[str, Any], native_id: str) -> str:
    refs = (swap_request.get("transactionRefsBySwapRequestId") or {}).get("nodes") or []
    for node in refs:
        ref = (node or {}).get("ref")
        if ref:
            return str(ref)
    return f"cf-{native_id}"


def parse_chainflip_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def extract_swap_request(raw_node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_node, dict):
        return None
    request = raw_node.get("swapRequest") or raw_node.get("swapRequestBySwapRequestId")
    return request if isinstance(request, dict) else None


def volume_usd_from_request(request: Dict[str, Any]) -> float:
    deposit = parse_decimal(request.get("depositValueUsd"))
    if deposit > 0:
        return deposit
    legs = (request.get("swapsBySwapRequestId") or {}).get("nodes") or []
    return sum(parse_decimal((leg or {}).get("swapInputValueUsd")) for leg in legs)


class ChainflipIngestor(BaseIngestor):
    def __init__(self):
        super().__init__(SYNC_SOURCE)
        self.graphql_url = allowlisted_graphql_url(config.CHAINFLIP_GRAPHQL_URL)
        self.broker_ss58 = config.CHAINFLIP_BROKER_SS58

    def fetch_data(self, **kwargs) -> Dict:
        return self.fetch_page(kwargs.get("next_page_token"))

    def fetch_page(self, after: Optional[str] = None) -> Dict[str, Any]:
        payload = {
            "query": BROKER_SWAPS_QUERY,
            "variables": {
                "idSs58": self.broker_ss58,
                "first": PAGE_SIZE,
                "after": after,
            },
        }
        response = self.session.post(
            self.graphql_url,
            json=payload,
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        delay = config.API_DELAYS.get(self.source_name, 0)
        if delay:
            time.sleep(delay)
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"Chainflip GraphQL errors: {body['errors']}")
        return body.get("data") or {}

    def parse_swap(self, raw_swap: Dict) -> Optional[Dict]:
        request = extract_swap_request(raw_swap)
        if not request:
            return None
        if not request.get("completedBlockTimestamp"):
            return None
        native_id = str(request.get("nativeId") or "")
        if not native_id:
            return None
        timestamp = parse_chainflip_timestamp(
            request.get("completedBlockTimestamp")
        ) or parse_chainflip_timestamp(request.get("requestedBlockTimestamp"))
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        in_asset = str(request.get("sourceAsset") or "")
        out_asset = str(request.get("destinationAsset") or "")
        volume_usd = volume_usd_from_request(request)
        user = request.get("sourceAddress") or request.get("destinationAddress") or ""
        draft = SwapKitVolumeDraft(
            tool=CHAINFLIP_TOOL,
            timestamp=timestamp,
            tx_hash=first_tx_ref(request, native_id),
            user_address=str(user),
            in_asset=in_asset,
            out_asset=out_asset,
            in_amount=amount_from_raw(request.get("depositAmount"), in_asset),
            out_amount=0.0,
            in_amount_usd=volume_usd,
            out_amount_usd=volume_usd,
            in_amount_raw=str(request.get("depositAmount") or "0"),
            raw_data=raw_swap,
        )
        return build_swapkit_swap_record(draft)

    def _page_nodes(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        account = data.get("account") or {}
        swaps = account.get("swaps") or {}
        return [node for node in (swaps.get("nodes") or []) if isinstance(node, dict)]

    def _page_cursor(self, data: Dict[str, Any]) -> Optional[str]:
        account = data.get("account") or {}
        page_info = (account.get("swaps") or {}).get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            return None
        return page_info.get("endCursor")

    def ingest(self) -> Dict[str, Any]:
        inserted = 0
        latest_ts = None
        cursor = None
        try:
            if not self.broker_ss58:
                return _result(inserted, latest_ts, "CHAINFLIP_BROKER_SS58 is empty")
            for _ in range(MAX_PAGES_PER_SYNC):
                data = self.fetch_page(cursor)
                if data.get("account") is None:
                    return _result(inserted, latest_ts, "Chainflip broker account not found")
                records = [row for row in (self.parse_swap(n) for n in self._page_nodes(data)) if row]
                if records:
                    inserted += self._insert_swaps(records)
                    if latest_ts is None:
                        latest_ts = records[0].get("timestamp")
                cursor = self._page_cursor(data)
                if not cursor:
                    break
            return _result(inserted, latest_ts, None)
        except Exception as exc:
            logger.error("Chainflip ingest failed: %s", exc)
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
