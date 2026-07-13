"""Etherscan ingestor for VULT buybacks made by the buyback wallet."""
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import psycopg2
import requests
from psycopg2.extras import execute_values

from config import config


logger = logging.getLogger(__name__)

ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
PAGE_SIZE = 1000
MAX_PAGES = 20
ETHEREUM_CHAIN_ID = 1
USDC_DECIMALS = 6
VULT_DECIMALS = 18
ETHERSCAN_TIMEOUT_SECONDS = 30
INSERT_PAGE_SIZE = 500

BUYBACK_TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS buyback_trades (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    tx_hash VARCHAR(66) UNIQUE NOT NULL,
    block_number BIGINT NOT NULL,
    usdc_spent NUMERIC(38, 6) NOT NULL,
    vult_bought NUMERIC(38, 18) NOT NULL,
    price NUMERIC(38, 18) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_buyback_trades_date ON buyback_trades(date DESC);
"""


@dataclass(frozen=True)
class BuybackTrade:
    date: date
    tx_hash: str
    block_number: int
    usdc_spent: Decimal
    vult_bought: Decimal
    price: Decimal

    def as_row(self) -> Tuple[date, str, int, Decimal, Decimal, Decimal]:
        return (
            self.date,
            self.tx_hash,
            self.block_number,
            self.usdc_spent,
            self.vult_bought,
            self.price,
        )


def _token_amount(transfer: Dict, decimals: int) -> Decimal:
    return Decimal(int(transfer["value"])) / (Decimal(10) ** decimals)


def _transfer_date(transfer: Dict) -> date:
    return datetime.fromtimestamp(int(transfer["timeStamp"]), timezone.utc).date()


def build_buyback_trades(transfers: List[Dict]) -> List[BuybackTrade]:
    """Pair canonical USDC outflows with canonical VULT inflows by tx hash."""
    wallet = config.BUYBACK_WALLET_ADDRESS.lower()
    usdc = config.USDC_ADDRESS.lower()
    vult = config.VULT_ADDRESS.lower()
    grouped: Dict[str, Dict] = {}

    for transfer in transfers:
        tx_hash = transfer.get("hash")
        if not tx_hash:
            continue
        trade = grouped.setdefault(tx_hash, {"usdc": Decimal(), "vult": Decimal(), "transfer": transfer})
        token = (transfer.get("contractAddress") or "").lower()
        sender = (transfer.get("from") or "").lower()
        recipient = (transfer.get("to") or "").lower()
        if token == usdc and sender == wallet:
            trade["usdc"] += _token_amount(transfer, USDC_DECIMALS)
        if token == vult and recipient == wallet:
            trade["vult"] += _token_amount(transfer, VULT_DECIMALS)

    trades = []
    for tx_hash, trade in grouped.items():
        if not trade["usdc"] or not trade["vult"]:
            continue
        transfer = trade["transfer"]
        trades.append(BuybackTrade(
            date=_transfer_date(transfer),
            tx_hash=tx_hash,
            block_number=int(transfer["blockNumber"]),
            usdc_spent=trade["usdc"],
            vult_bought=trade["vult"],
            price=trade["usdc"] / trade["vult"],
        ))
    return trades


class BuybackIngestor:
    """Persist Etherscan-confirmed VULT buybacks for the transparency page."""

    def __init__(self):
        if not ETHERSCAN_API_KEY:
            raise ValueError("ETHERSCAN_API_KEY environment variable not set")
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")
        self.api_key = ETHERSCAN_API_KEY
        self.database_url = DATABASE_URL
        self.db: Optional[psycopg2.extensions.connection] = None
        self.delay = config.API_DELAYS.get("etherscan", 0.25)

    def _get_connection(self):
        try:
            if self.db and not self.db.closed:
                cursor = self.db.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return self.db
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("Database connection lost, reconnecting")
        self.db = psycopg2.connect(self.database_url)
        return self.db

    def _ensure_schema(self) -> None:
        db = self._get_connection()
        cursor = db.cursor()
        try:
            cursor.execute(BUYBACK_TRADES_SCHEMA)
            db.commit()
        finally:
            cursor.close()

    def _last_block(self) -> Optional[int]:
        db = self._get_connection()
        cursor = db.cursor()
        try:
            cursor.execute("SELECT MAX(block_number) FROM buyback_trades")
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else None
        finally:
            cursor.close()

    def _fetch_page(self, startblock: int, page: int) -> Tuple[List[Dict], Optional[str]]:
        params = {
            "chainid": ETHEREUM_CHAIN_ID,
            "module": "account",
            "action": "tokentx",
            "address": config.BUYBACK_WALLET_ADDRESS,
            "startblock": startblock,
            "page": page,
            "offset": PAGE_SIZE,
            "sort": "asc",
            "apikey": self.api_key,
        }
        try:
            response = requests.get(
                ETHERSCAN_V2_URL,
                params=params,
                timeout=ETHERSCAN_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as error:
            return [], f"Etherscan request failed: {error}"

        result = data.get("result", [])
        if data.get("status") == "1" and isinstance(result, list):
            return result, None
        is_empty = result == [] or (
            data.get("message") == "NOTOK" and result == "NOTOK"
        )
        if data.get("status") == "0" and is_empty:
            return [], None
        return [], f"Etherscan response: {data.get('message')!r} {result!r}"

    def _fetch_transfers(self) -> Tuple[List[Dict], Optional[str]]:
        last_block = self._last_block()
        startblock = last_block or 0
        transfers: List[Dict] = []
        for page in range(1, MAX_PAGES + 1):
            result, error = self._fetch_page(startblock, page)
            if error:
                return [], error
            transfers.extend(result)
            if len(result) < PAGE_SIZE:
                return transfers, None
            time.sleep(self.delay)
        return [], f"Etherscan result exceeded {MAX_PAGES} pages"

    def _insert_trades(self, trades: List[BuybackTrade]) -> int:
        if not trades:
            return 0
        db = self._get_connection()
        cursor = db.cursor()
        sql = """
            INSERT INTO buyback_trades
                (date, tx_hash, block_number, usdc_spent, vult_bought, price)
            VALUES %s
            ON CONFLICT (tx_hash) DO NOTHING
        """
        try:
            execute_values(cursor, sql, [trade.as_row() for trade in trades], page_size=INSERT_PAGE_SIZE)
            return cursor.rowcount
        finally:
            cursor.close()

    def ingest(self) -> Dict[str, object]:
        result: Dict[str, object] = {"inserted": 0, "error": None}
        try:
            self._ensure_schema()
            transfers, error = self._fetch_transfers()
            if error:
                result["error"] = error
                return result
            result["inserted"] = self._insert_trades(build_buyback_trades(transfers))
            self._get_connection().commit()
        except Exception as error:
            logger.exception("Buyback ingestion failed")
            if self.db:
                self.db.rollback()
            result["error"] = str(error)
        finally:
            if self.db:
                self.db.close()
                self.db = None
        return result
