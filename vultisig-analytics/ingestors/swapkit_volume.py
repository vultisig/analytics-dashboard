"""Shared SwapKit volume row builder.

Protocol APIs write volume/count only. Revenue stays on the EVM fee-wallet
receipts. Callers cannot choose `source` — it is always `swapkit`.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from config import config
from ingestors.base import classify_volume_tier

SWAPKIT_PLATFORM = "All"
ZERO_AFFILIATE_FEE_USD = 0.0


@dataclass(frozen=True)
class SwapKitVolumeDraft:
    tool: str
    timestamp: datetime
    tx_hash: str
    user_address: str
    in_asset: str
    out_asset: str
    in_amount: float
    out_amount: float
    in_amount_usd: float
    out_amount_usd: float
    in_amount_raw: str
    raw_data: Dict[str, Any]
    block_height: Optional[int] = None
    platform: str = SWAPKIT_PLATFORM
    in_tx_id: Optional[str] = None


def parse_decimal(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def build_swapkit_swap_record(draft: SwapKitVolumeDraft) -> Dict[str, Any]:
    source = config.SWAPKIT_PROTOCOL
    return {
        "timestamp": draft.timestamp,
        "date_only": draft.timestamp.date(),
        "source": source,
        "tool": draft.tool,
        "tx_hash": draft.tx_hash,
        "block_height": draft.block_height,
        "user_address": draft.user_address or "",
        "in_asset": draft.in_asset,
        "in_amount": draft.in_amount,
        "in_amount_usd": draft.in_amount_usd,
        "out_asset": draft.out_asset,
        "out_amount": draft.out_amount,
        "out_amount_usd": draft.out_amount_usd,
        "total_fee_usd": 0.0,
        "network_fee_usd": 0.0,
        "liquidity_fee_usd": 0.0,
        "affiliate_fee_usd": ZERO_AFFILIATE_FEE_USD,
        "pool_1": f"{draft.in_asset}-{draft.out_asset}",
        "pool_2": None,
        "is_streaming_swap": False,
        "swap_slip": None,
        "volume_tier": classify_volume_tier(draft.in_amount_usd),
        "platform": draft.platform,
        "raw_data": json.dumps(draft.raw_data),
        "in_address": draft.user_address or "",
        "in_tx_id": draft.in_tx_id or draft.tx_hash,
        "in_amount_raw": draft.in_amount_raw,
        "out_addresses": json.dumps([draft.user_address] if draft.user_address else []),
        "out_tx_ids": None,
        "out_heights": None,
        "affiliate_addresses": None,
        "affiliate_fees_bps": None,
        "metadata_complete": None,
        "in_price_usd": None,
        "out_price_usd": None,
        "network_fees_raw": None,
        "pools_used": None,
        "swap_status": "success",
        "swap_type": draft.tool,
        "memo": None,
    }
