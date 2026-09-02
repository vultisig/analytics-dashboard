"""Near-Intents affiliate fees accrued per app, read keylessly from `intents.near`.

Fees never settle to `vultisigswapkit.near`; they sit as NEP-245 balances
credited to one implicit account per app until Near sweeps them to the
EVM fee wallet as a lump sum. Snapshotting those balances is the only
per-platform view of Near revenue. Only stablecoin balances get a USD
value; everything else is stored raw and left unpriced.
"""

import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from config import config
from ingestors.base import BaseIngestor

logger = logging.getLogger(__name__)

SYNC_SOURCE = "near-intents-accrual"
PROVIDER = "near-intents"
INTENTS_CONTRACT = "intents.near"
TOKEN_PAGE_LIMIT = 100
# Balances move rarely; one snapshot per hour keeps the table honest and small.
SNAPSHOT_BUCKET_SECONDS = 3600
STABLE_TOKEN_DECIMALS = {
    "nep141:eth-0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.omft.near": 6,  # USDC (Ethereum)
    "nep141:17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1": 6,  # USDC (NEAR)
    "nep141:eth-0xdac17f958d2ee523a2206206994597c13d831ec7.omft.near": 6,  # USDT (Ethereum)
}

AccrualRow = Dict[str, Any]


def snapshot_bucket(now: datetime) -> datetime:
    epoch = int(now.timestamp()) // SNAPSHOT_BUCKET_SECONDS * SNAPSHOT_BUCKET_SECONDS
    return datetime.fromtimestamp(epoch, timezone.utc)


def stable_usd(token_id: str, amount_raw: int) -> Optional[float]:
    decimals = STABLE_TOKEN_DECIMALS.get(token_id)
    if decimals is None:
        return None
    return amount_raw / (10 ** decimals)


def build_accrual_rows(snapshot_at: datetime, platform: str, balances: List[Tuple[str, str]]) -> List[AccrualRow]:
    rows = []
    for token_id, raw in balances:
        amount_raw = int(raw or 0)
        if amount_raw <= 0:
            continue
        rows.append({
            "snapshot_at": snapshot_at,
            "provider": PROVIDER,
            "platform": platform,
            "token_id": token_id,
            "amount_raw": amount_raw,
            "amount_usd": stable_usd(token_id, amount_raw),
        })
    return rows


class NearIntentsAccrualReader(BaseIngestor):
    def __init__(self):
        super().__init__(SYNC_SOURCE)
        self.rpc_url = config.NEAR_RPC_URL
        self.accounts = tuple(config.NEAR_INTENTS_APP_ACCOUNTS)

    def fetch_data(self, **kwargs) -> Dict:
        return {"balances": self.fetch_balances(kwargs["account"])}

    def parse_swap(self, raw_swap: Dict) -> Dict:
        raise NotImplementedError("accrual reader writes balances, not swaps")

    def call_view(self, method: str, args: Dict[str, Any]) -> Any:
        body = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "query",
            "params": {
                "request_type": "call_function",
                "finality": "final",
                "account_id": INTENTS_CONTRACT,
                "method_name": method,
                "args_base64": base64.b64encode(json.dumps(args).encode()).decode(),
            },
        }
        response = self.session.post(self.rpc_url, json=body, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") or {}
        error = payload.get("error") or result.get("error")
        if error:
            raise RuntimeError(f"NEAR view {method} failed: {error}")
        return json.loads(bytes(result["result"]).decode())

    def fetch_token_ids(self, account: str) -> List[str]:
        token_ids: List[str] = []
        while True:
            page = self.call_view(
                "mt_tokens_for_owner",
                {"account_id": account, "from_index": str(len(token_ids)), "limit": TOKEN_PAGE_LIMIT},
            )
            token_ids.extend(t["token_id"] for t in page)
            if len(page) < TOKEN_PAGE_LIMIT:
                return token_ids

    def fetch_balances(self, account: str) -> List[Tuple[str, str]]:
        token_ids = self.fetch_token_ids(account)
        if not token_ids:
            return []
        balances = self.call_view("mt_batch_balance_of", {"account_id": account, "token_ids": token_ids})
        return list(zip(token_ids, balances))

    def ingest(self, state_token: Optional[str] = None) -> Dict[str, Any]:
        snapshot_at = snapshot_bucket(datetime.now(timezone.utc))
        rows: List[AccrualRow] = []
        errors: List[str] = []
        for platform, account in self.accounts:
            try:
                rows.extend(build_accrual_rows(snapshot_at, platform, self.fetch_balances(account)))
            except Exception as exc:  # one app's RPC failure must not hide the others
                errors.append(f"{platform}: {exc}")
        inserted = self._insert_accruals(rows) if rows else 0
        return {
            "source": SYNC_SOURCE,
            "inserted": inserted,
            "pages": len(self.accounts),
            "latest_ts": snapshot_at if rows else None,
            "error": "; ".join(errors) or None,
            "next_state": None,
        }

    @staticmethod
    def _insert_accruals(rows: List[AccrualRow]) -> int:
        from database.connection import db_manager
        return db_manager.insert_swapkit_accruals(rows)
