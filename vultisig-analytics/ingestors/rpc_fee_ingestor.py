"""Fee-wallet receipts on EVM chains Etherscan V2 does not index (Robinhood).

Same table and row shape as the Etherscan crawl, sourced from the chain's
own RPC: ERC-20 Transfer logs into each fee receiver, classified at ingest
by `tx.to` against the known routers, so the router classifier (which needs
Etherscan) never has to touch these rows. Progress is the last scanned
block per chain, persisted through `sync_status.next_page_token`.
"""

import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import config
from ingestors.protocol_identifier import KNOWN_ROUTERS
from ingestors.swapkit_senders import OTHER_PROTOCOL, protocol_for_sender

logger = logging.getLogger(__name__)

SYNC_SOURCE = "rpc-fees"
FEE_DATA_SOURCE = "rpc"
CLASSIFIED_STAMP = "router_check"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SYMBOL_SELECTOR = "0x95d89b41"
DECIMALS_SELECTOR = "0x313ce567"
RETRY_ATTEMPTS = 5
RETRY_BASE_SECONDS = 3
WORD_BYTES = 32

INSERT_COLUMNS = (
    'tx_hash', 'chain', 'protocol', 'timestamp',
    'actual_fee_usd', 'fee_token_symbol', 'fee_token_address', 'fee_amount_raw',
    'swap_volume_usd', 'token_in_symbol', 'token_in_address',
    'token_out_symbol', 'token_out_address',
    'amount_in', 'amount_out',
    'block_number', 'from_address', 'to_address',
    'fee_data_source', 'volume_data_source',
)

ROUTER_TO_PROTOCOL = {
    address.lower(): protocol
    for protocol, addresses in KNOWN_ROUTERS.items()
    for address in addresses
}


def topic_address(address: str) -> str:
    return "0x" + address[2:].lower().rjust(WORD_BYTES * 2, "0")


def address_from_topic(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def decode_string(hex_data: str) -> str:
    raw = bytes.fromhex(hex_data[2:])
    if len(raw) == WORD_BYTES:  # bytes32-style symbol
        return raw.rstrip(b"\x00").decode(errors="replace")
    length = int.from_bytes(raw[WORD_BYTES:WORD_BYTES * 2], "big")
    return raw[WORD_BYTES * 2:WORD_BYTES * 2 + length].decode(errors="replace")


def classify_protocol(source_name: str, tx_to: Optional[str], from_address: str) -> str:
    """Router match keeps the receiver's provider; known senders keep their owner; else other."""
    if ROUTER_TO_PROTOCOL.get((tx_to or "").lower()) == source_name:
        return source_name
    return protocol_for_sender(from_address) or OTHER_PROTOCOL


def build_row(
    log: Dict[str, Any],
    source_name: str,
    chain_name: str,
    timestamp: datetime,
    tx_to: Optional[str],
    token: Tuple[str, int],
) -> Tuple:
    symbol, decimals = token
    raw_amount = int(log["data"], 16)
    human = Decimal(raw_amount) / (Decimal(10) ** decimals) if decimals else Decimal(raw_amount)
    from_address = address_from_topic(log["topics"][1])
    return (
        log["transactionHash"],
        chain_name,
        classify_protocol(source_name, tx_to, from_address),
        timestamp,
        0,  # actual_fee_usd — enricher prices it when the token is known
        symbol,
        log["address"].lower(),
        format(human, 'f'),
        None,
        None, None, None, None,
        human,
        None,
        int(log["blockNumber"], 16),
        from_address,
        address_from_topic(log["topics"][2]),
        FEE_DATA_SOURCE,
        CLASSIFIED_STAMP,
    )


class RpcClient:
    def __init__(self, url: str, session: requests.Session, delay: float):
        self.url = url
        self.session = session
        self.delay = delay

    def call(self, method: str, params: List[Any]) -> Any:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(RETRY_ATTEMPTS):
            response = self.session.post(self.url, json=body, timeout=config.REQUEST_TIMEOUT)
            if response.status_code == 429:  # public RPCs throttle hard; back off, do not fail the sync
                time.sleep(RETRY_BASE_SECONDS * (attempt + 1))
                continue
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(f"RPC {method} failed: {payload['error']}")
            time.sleep(self.delay)
            return payload["result"]
        raise RuntimeError(f"RPC {method} rate-limited after {RETRY_ATTEMPTS} attempts")

    def latest_block(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def transfer_logs(self, receiver: str, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        return self.call("eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [TRANSFER_TOPIC, None, topic_address(receiver)],
        }])

    def block_timestamp(self, block_hex: str) -> datetime:
        block = self.call("eth_getBlockByNumber", [block_hex, False])
        return datetime.fromtimestamp(int(block["timestamp"], 16), timezone.utc)

    def tx_to(self, tx_hash: str) -> Optional[str]:
        tx = self.call("eth_getTransactionByHash", [tx_hash])
        return (tx or {}).get("to")

    def token_meta(self, token: str) -> Tuple[str, int]:
        symbol = decode_string(self.call("eth_call", [{"to": token, "data": SYMBOL_SELECTOR}, "latest"]))
        decimals = int(self.call("eth_call", [{"to": token, "data": DECIMALS_SELECTOR}, "latest"]), 16)
        return symbol, decimals


class RpcFeeIngestor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'VultisigAnalytics/1.0'})
        self.chains = tuple(config.RPC_FEE_CHAINS)
        self.integrators = tuple(config.ARKHAM_FEE_RECEIVERS)
        self.delay = config.API_DELAYS.get(SYNC_SOURCE, 1.0)

    def client(self, url: str) -> RpcClient:
        return RpcClient(url, self.session, self.delay)

    def scan_chain(self, rpc: RpcClient, chain_name: str, from_block: int, to_block: int) -> List[Tuple]:
        rows: List[Tuple] = []
        timestamps: Dict[str, datetime] = {}
        tokens: Dict[str, Tuple[str, int]] = {}
        for source_name, receiver in self.integrators:
            for log in rpc.transfer_logs(receiver, from_block, to_block):
                block_hex = log["blockNumber"]
                if block_hex not in timestamps:
                    timestamps[block_hex] = rpc.block_timestamp(block_hex)
                token = log["address"].lower()
                if token not in tokens:
                    tokens[token] = rpc.token_meta(token)
                tx_to = rpc.tx_to(log["transactionHash"])
                rows.append(build_row(log, source_name, chain_name, timestamps[block_hex], tx_to, tokens[token]))
        return rows

    def first_block(self, chain_name: str, head: int, lookback: int) -> int:
        """Resume after whatever any earlier crawl stored for this chain, else look back."""
        stored = self._last_block_in_db(chain_name)
        return stored + 1 if stored else head - lookback + 1

    def catch_up(self, rpc: RpcClient, chain_name: str, start: int, head: int, window: int) -> Tuple[List[Tuple], int]:
        """Walk up to RPC_FEE_MAX_WINDOWS_PER_SYNC windows toward head; returns rows and the last block scanned."""
        rows: List[Tuple] = []
        stop = start - 1
        for _ in range(config.RPC_FEE_MAX_WINDOWS_PER_SYNC):
            if stop >= head:
                break
            window_start = stop + 1
            stop = min(head, window_start + window - 1)
            rows.extend(self.scan_chain(rpc, chain_name, window_start, stop))
        return rows, stop

    def ingest(self, state_token: Optional[str] = None) -> Dict[str, Any]:
        state = json.loads(state_token) if state_token else {}
        last_blocks: Dict[str, int] = dict(state.get("last_block") or {})
        inserted = 0
        latest_ts: Optional[datetime] = None
        errors: List[str] = []
        for _chain_id, chain_name, url, window, lookback in self.chains:
            rpc = self.client(url)
            try:
                head = rpc.latest_block()
                start = last_blocks[chain_name] + 1 if chain_name in last_blocks else self.first_block(chain_name, head, lookback)
                rows, stop = self.catch_up(rpc, chain_name, start, head, window)
                inserted += self._insert_rows(rows)
                last_blocks[chain_name] = stop
                if rows:
                    newest = max(row[3] for row in rows)
                    latest_ts = newest if latest_ts is None else max(latest_ts, newest)
            except Exception as exc:  # one chain's RPC outage must not hide the others' progress
                errors.append(f"{chain_name}: {exc}")
        return {
            "source": SYNC_SOURCE,
            "inserted": inserted,
            "pages": len(self.chains),
            "latest_ts": latest_ts,
            "error": "; ".join(errors) or None,
            "next_state": json.dumps({"last_block": last_blocks}, sort_keys=True),
        }

    @staticmethod
    def _last_block_in_db(chain_name: str) -> Optional[int]:
        from database.connection import db_manager
        rows = db_manager.execute_query(
            "SELECT MAX(block_number) AS last_block FROM dex_aggregator_revenue WHERE chain = %s",
            (chain_name,),
            fetch=True,
        )
        return rows[0]["last_block"] if rows and rows[0]["last_block"] is not None else None

    @staticmethod
    def _insert_rows(rows: List[Tuple]) -> int:
        if not rows:
            return 0
        from psycopg2.extras import execute_values
        from database.connection import db_manager
        cols = ', '.join(INSERT_COLUMNS)
        sql = f"INSERT INTO dex_aggregator_revenue ({cols}) VALUES %s ON CONFLICT (tx_hash) DO NOTHING"
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=500)
                conn.commit()
                return cur.rowcount
