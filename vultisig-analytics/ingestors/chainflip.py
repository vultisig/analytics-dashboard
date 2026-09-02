"""Chainflip volume for the SwapKit affiliate accounts (not the submitting broker).

SwapKit issues one Chainflip affiliate account per app, so the account is
the platform. Rows are keyed on the Chainflip nativeId — on-chain refs can
appear after completion and would otherwise re-key the same swap.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from config import config
from ingestors.base import BaseIngestor
from ingestors.paged_volume_sync import Page, ingest_feeds
from ingestors.swapkit_volume import (
    SwapKitVolumeDraft,
    build_swapkit_swap_record,
    parse_decimal,
)

logger = logging.getLogger(__name__)

CHAINFLIP_TOOL = "chainflip"
SYNC_SOURCE = "chainflip"
PAGE_SIZE = 50
DEFAULT_ASSET_DECIMALS = 18
ASSET_DECIMALS = {
    "Usdc": 6,
    "Usdt": 6,
    "Btc": 8,
    "Sol": 9,
    "Dot": 10,
    "HubDot": 10,
}

AFFILIATE_SWAPS_QUERY = """
query AffiliateSwaps($idSs58: String!, $first: Int!, $after: Cursor) {
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
          totalBrokerCommissionBps
          egress: egressByEgressId { amount valueUsd }
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


def first_tx_ref(swap_request: Dict[str, Any]) -> Optional[str]:
    refs = (swap_request.get("transactionRefsBySwapRequestId") or {}).get("nodes") or []
    for node in refs:
        ref = (node or {}).get("ref")
        if ref:
            return str(ref)
    return None


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


def leg_usd_sum(request: Dict[str, Any], leg_field: str) -> float:
    legs = (request.get("swapsBySwapRequestId") or {}).get("nodes") or []
    return sum(parse_decimal((leg or {}).get(leg_field)) for leg in legs)


def volume_usd_from_request(request: Dict[str, Any]) -> float:
    deposit = parse_decimal(request.get("depositValueUsd"))
    return deposit if deposit > 0 else leg_usd_sum(request, "swapInputValueUsd")


def egress_from_request(request: Dict[str, Any], out_asset: str) -> Tuple[float, float]:
    egress = request.get("egress") or {}
    out_amount = amount_from_raw(egress.get("amount"), out_asset)
    out_usd = parse_decimal(egress.get("valueUsd"))
    if out_usd <= 0:
        out_usd = leg_usd_sum(request, "swapOutputValueUsd")
    return out_amount, out_usd


class ChainflipIngestor(BaseIngestor):
    def __init__(self):
        super().__init__(SYNC_SOURCE)
        self.graphql_url = allowlisted_graphql_url(config.CHAINFLIP_GRAPHQL_URL)
        self.accounts = tuple(config.CHAINFLIP_AFFILIATE_ACCOUNTS)

    def fetch_data(self, **kwargs) -> Dict:
        return self.fetch_page(kwargs["account"], kwargs.get("next_page_token"))

    def fetch_page(self, account: str, after: Optional[str] = None) -> Dict[str, Any]:
        payload = {
            "query": AFFILIATE_SWAPS_QUERY,
            "variables": {"idSs58": account, "first": PAGE_SIZE, "after": after},
        }
        response = self.session.post(self.graphql_url, json=payload, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        delay = config.API_DELAYS.get(self.source_name, 0)
        if delay:
            time.sleep(delay)
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"Chainflip GraphQL errors: {body['errors']}")
        return body.get("data") or {}

    def parse_swap(self, raw_swap: Dict, platform: str = "All") -> Optional[Dict]:
        request = extract_swap_request(raw_swap)
        if not request or not request.get("completedBlockTimestamp"):
            return None
        native_id = str(request.get("nativeId") or "")
        if not native_id:
            return None
        timestamp = parse_chainflip_timestamp(request.get("completedBlockTimestamp"))
        if timestamp is None:
            # Timestamp is part of the row key; a substitute would re-insert this swap every sync.
            logger.warning("Chainflip swap %s has unreadable completedBlockTimestamp; skipped", native_id)
            return None
        in_asset = str(request.get("sourceAsset") or "")
        out_asset = str(request.get("destinationAsset") or "")
        out_amount, out_usd = egress_from_request(request, out_asset)
        user = request.get("sourceAddress") or request.get("destinationAddress") or ""
        draft = SwapKitVolumeDraft(
            tool=CHAINFLIP_TOOL,
            timestamp=timestamp,
            tx_hash=f"cf-{native_id}",
            user_address=str(user),
            in_asset=in_asset,
            out_asset=out_asset,
            in_amount=amount_from_raw(request.get("depositAmount"), in_asset),
            out_amount=out_amount,
            in_amount_usd=volume_usd_from_request(request),
            out_amount_usd=out_usd,
            in_amount_raw=str(request.get("depositAmount") or "0"),
            raw_data=raw_swap,
            platform=platform,
            in_tx_id=first_tx_ref(request),
        )
        return build_swapkit_swap_record(draft)

    def parsed_page(self, platform: str, account: str, after: Optional[str]) -> Page:
        data = self.fetch_page(account, after)
        if data.get("account") is None:
            raise RuntimeError(f"Chainflip affiliate account not found: {account}")
        swaps = data["account"].get("swaps") or {}
        nodes = [node for node in (swaps.get("nodes") or []) if isinstance(node, dict)]
        records = [row for row in (self.parse_swap(n, platform) for n in nodes) if row]
        page_info = swaps.get("pageInfo") or {}
        next_cursor = page_info.get("endCursor") if page_info.get("hasNextPage") else None
        return records, next_cursor

    def ingest(self, state_token: Optional[str] = None) -> Dict[str, Any]:
        feeds = [
            (platform, self._feed(platform, account))
            for platform, account in self.accounts
            if account
        ]
        return ingest_feeds(SYNC_SOURCE, feeds, state_token, self._insert_swaps)

    def _feed(self, platform: str, account: str):
        return lambda after: self.parsed_page(platform, account, after)

    @staticmethod
    def _insert_swaps(records: List[Dict[str, Any]]) -> int:
        from database.connection import db_manager
        return db_manager.insert_swaps(records)
