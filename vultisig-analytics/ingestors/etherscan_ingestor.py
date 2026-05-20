"""
Etherscan V2 multichain ingestor for DEX aggregator fees.

Replaces ArkhamIngestor for the same data shape:
  - One row per ERC20 transfer landing on a Vultisig fee receiver
  - Multi-chain via Etherscan V2 unified API (`chainid=` param, single key)
  - Per-(source, chain) cursor on `block_number` so we never refetch

USD pricing is left to the enrichment pipeline — Etherscan returns raw token
amounts but no historical USD value. `actual_fee_usd` stays NULL until an
enricher pass (existing `enrich_from_1inch_api.py` or similar) fills it.
"""

import os
import logging
import time
import requests
import psycopg2
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from config import config

logger = logging.getLogger(__name__)

# Etherscan V2 multichain API. One key works across all listed chainids.
ETHERSCAN_V2_URL = 'https://api.etherscan.io/v2/api'
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

# (chainid, display_name) — keep `display_name` aligned with the existing
# `dex_aggregator_revenue.chain` column convention used by ArkhamIngestor +
# the frontend.
DEFAULT_CHAINS: List[Tuple[int, str]] = [
    (1, 'Ethereum'),
    (56, 'BSC'),
    (137, 'Polygon'),
    (42161, 'Arbitrum'),
    (10, 'Optimism'),
    (8453, 'Base'),
    (43114, 'Avalanche'),
    (81457, 'Blast'),
]

# Free-tier rate limit is 5 req/s shared across chainids. We sleep between
# calls to stay comfortably under that.
PER_REQUEST_DELAY_SECONDS = 0.25
PAGE_SIZE = 1000  # max Etherscan allows for tokentx
MAX_PAGES_PER_CHAIN = 20  # safety bound: 20 * 1000 = 20k transfers per (chain, source) per sync


class EtherscanIngestor:
    """Ingests aggregator fee transfers via Etherscan V2 multichain API."""

    def __init__(
        self,
        integrators: Optional[List[Tuple[str, str]]] = None,
        chains: Optional[List[Tuple[int, str]]] = None,
    ):
        if not ETHERSCAN_API_KEY:
            raise ValueError("ETHERSCAN_API_KEY environment variable not set")
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")

        self.api_key = ETHERSCAN_API_KEY
        self.database_url = DATABASE_URL
        self.db: Optional[psycopg2.extensions.connection] = None
        self.integrators = integrators if integrators is not None else config.ARKHAM_FEE_RECEIVERS
        self.chains = chains if chains is not None else DEFAULT_CHAINS

    def _get_connection(self):
        try:
            if self.db and not self.db.closed:
                cursor = self.db.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return self.db
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("Database connection lost, reconnecting...")
            if self.db:
                try:
                    self.db.close()
                except Exception:
                    pass

        self.db = psycopg2.connect(self.database_url)
        logger.info("Database connection established")
        return self.db

    def _last_block_for(self, source_name: str, chain_name: str) -> Optional[int]:
        """Highest block_number we've already ingested for (protocol, chain).
        Returning None means start from the chain genesis."""
        db = self._get_connection()
        cursor = db.cursor()
        try:
            cursor.execute(
                "SELECT MAX(block_number) FROM dex_aggregator_revenue "
                "WHERE protocol = %s AND chain = %s",
                (source_name, chain_name),
            )
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else None
        finally:
            cursor.close()

    def fetch_chain_page(
        self,
        chainid: int,
        address: str,
        startblock: int,
        page: int,
    ) -> Tuple[List[Dict], Optional[str]]:
        """One page of token transfers. Returns (transfers, error_msg).

        Distinguishes Etherscan's "No transactions found" (status=0, but
        legitimate empty result) from real errors (timeouts, 4xx/5xx,
        rate-limit, etc.) so the orchestrator can surface real failures."""
        params = {
            'chainid': chainid,
            'module': 'account',
            'action': 'tokentx',
            'address': address,
            'startblock': startblock,
            'page': page,
            'offset': PAGE_SIZE,
            'sort': 'asc',
            'apikey': self.api_key,
        }
        try:
            r = requests.get(ETHERSCAN_V2_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.RequestException as e:
            return [], f"HTTP error on chainid={chainid}: {e}"

        status = data.get('status')
        msg = data.get('message', '')
        result = data.get('result', [])

        if status == '1' and isinstance(result, list):
            return result, None
        # Etherscan returns status=0 with "No transactions found" when an
        # address simply has nothing on a given chain. Treat that as a clean
        # empty page, not an error.
        if status == '0' and ('No transactions' in msg or msg == 'No transactions found'):
            return [], None
        # Anything else is a real failure worth surfacing.
        return [], f"Etherscan chainid={chainid} status={status} msg={msg!r}"

    def insert_transfer(
        self,
        transfer: Dict,
        source_name: str,
        chain_name: str,
        receiver_address: str,
    ) -> bool:
        """Insert one Etherscan tokentx row. Returns True if a row landed
        (False if skipped — e.g. transfer wasn't actually TO the receiver,
        or the row is a duplicate)."""
        tx_hash = transfer.get('hash')
        to_addr = (transfer.get('to') or '').lower()
        if not tx_hash:
            logger.warning(f"tokentx missing hash on {chain_name}: {transfer}")
            return False
        # `tokentx?address=X` returns transfers where X is FROM or TO. We
        # only want fee deposits — drop the FROM-side (e.g. someone moving
        # fee tokens out of the receiver).
        if to_addr != receiver_address.lower():
            return False

        try:
            block_number = int(transfer.get('blockNumber', 0)) or None
            timestamp_unix = int(transfer.get('timeStamp', 0))
            timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc) if timestamp_unix else datetime.now(tz=timezone.utc)

            decimals = int(transfer.get('tokenDecimal', 0)) if transfer.get('tokenDecimal') else 0
            raw_value = int(transfer.get('value', 0)) if transfer.get('value') else 0
            human_amount = raw_value / (10 ** decimals) if decimals else float(raw_value)

            from_address = (transfer.get('from') or '').lower()
            fee_token_symbol = transfer.get('tokenSymbol') or ''
            fee_token_address = transfer.get('contractAddress') or ''
            fee_amount_raw = str(human_amount)

            db = self._get_connection()
            cursor = db.cursor()
            cursor.execute(
                """
                INSERT INTO dex_aggregator_revenue (
                    tx_hash, chain, protocol, timestamp,
                    actual_fee_usd, fee_token_symbol, fee_token_address, fee_amount_raw,
                    swap_volume_usd, token_in_symbol, token_in_address,
                    token_out_symbol, token_out_address,
                    amount_in, amount_out,
                    block_number, from_address, to_address,
                    fee_data_source, volume_data_source
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
                ) ON CONFLICT (tx_hash) DO NOTHING
                """,
                (
                    tx_hash, chain_name, source_name, timestamp,
                    None, fee_token_symbol, fee_token_address, fee_amount_raw,
                    None, None, None,
                    None, None,
                    human_amount, None,
                    block_number, from_address, to_addr,
                    'etherscan', None,
                ),
            )
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"insert_transfer failed for tx={tx_hash}: {e}")
            if self.db:
                self.db.rollback()
            return False

    def ingest_chain(
        self,
        source_name: str,
        receiver_address: str,
        chainid: int,
        chain_name: str,
    ) -> Tuple[int, Optional[str]]:
        """Paginate this chain from the per-(source, chain) cursor. Returns
        (inserted_count, error_msg)."""
        last_block = self._last_block_for(source_name, chain_name)
        startblock = (last_block + 1) if last_block else 0
        inserted = 0

        for page in range(1, MAX_PAGES_PER_CHAIN + 1):
            transfers, err = self.fetch_chain_page(chainid, receiver_address, startblock, page)
            if err:
                return inserted, err
            if not transfers:
                break

            page_inserts = sum(
                1 for t in transfers
                if self.insert_transfer(t, source_name, chain_name, receiver_address)
            )
            inserted += page_inserts
            self._get_connection().commit()
            logger.info(
                f"  {source_name}/{chain_name} page={page}: {page_inserts} new "
                f"(of {len(transfers)} returned)"
            )

            # Last page is shorter than PAGE_SIZE → end of stream.
            if len(transfers) < PAGE_SIZE:
                break

            time.sleep(PER_REQUEST_DELAY_SECONDS)

        return inserted, None

    def ingest_one(self, source_name: str, receiver_address: str) -> Dict:
        """Walk every chain for one (source, receiver). Per-chain errors
        accumulate into the result so the orchestrator can surface them in
        sync_status."""
        result = {'source': source_name, 'inserted': 0, 'error': None}
        errors: List[str] = []
        try:
            self._get_connection()
            for chainid, chain_name in self.chains:
                count, err = self.ingest_chain(source_name, receiver_address, chainid, chain_name)
                result['inserted'] += count
                if err:
                    errors.append(f"{chain_name}: {err}")
                time.sleep(PER_REQUEST_DELAY_SECONDS)
            if errors:
                result['error'] = '; '.join(errors)
            logger.info(
                f"Completed {source_name}: {result['inserted']} new rows "
                f"({len(errors)} chain errors)"
            )
        except Exception as e:
            logger.error(f"ingest_one crashed for {source_name}: {e}")
            result['error'] = str(e)
            if self.db:
                self.db.rollback()
        return result

    def ingest(self) -> List[Dict]:
        """Run all configured (source, receiver) pairs. One result per source."""
        results: List[Dict] = []
        try:
            self._get_connection()
            for source_name, address in self.integrators:
                logger.info(f"=== Etherscan ingestion: {source_name} ({address}) ===")
                results.append(self.ingest_one(source_name, address))
            return results
        finally:
            if self.db:
                self.db.close()
                self.db = None


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ingestor = EtherscanIngestor()
    results = ingestor.ingest()
    for r in results:
        print(r)


if __name__ == '__main__':
    main()
