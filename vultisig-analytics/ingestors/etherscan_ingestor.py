"""Fee-receiver ingestor for DEX aggregator fees (Etherscan V2 or Alchemy).

With ALCHEMY_API_KEY set, every chain in config.ALCHEMY_NETWORKS is crawled
with alchemy_getAssetTransfers, which also returns native-coin credits
(1inch and KyberSwap pay the fee in the output token, which can be the
chain's native coin). Without the key, the Etherscan V2 `tokentx` crawl runs
as before (ERC-20 only).

Rows from both crawls carry fee_data_source = 'etherscan', which every
consumer reads as "on-chain transfer into a fee receiver".
"""

import os
import logging
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from config import config

logger = logging.getLogger(__name__)

ETHERSCAN_V2_URL = 'https://api.etherscan.io/v2/api'
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

# (chainid, dex_aggregator_revenue.chain) — display names align with frontend.
DEFAULT_CHAINS: List[Tuple[int, str]] = [
    (1, 'Ethereum'),
    (56, 'BSC'),
    (137, 'Polygon'),
    (42161, 'Arbitrum'),
    (10, 'Optimism'),
    (8453, 'Base'),
    (43114, 'Avalanche'),
    (81457, 'Blast'),
    (999, 'HyperEVM'),
]

PAGE_SIZE = 1000  # Etherscan tokentx cap
MAX_PAGES_PER_CHAIN = 20

ALCHEMY_PAGE_SIZE = 1000
# 'internal' carries native-coin fees paid out by a router contract. Some
# networks reject it; the crawl then retries the page with ERC-20 only.
ALCHEMY_CATEGORIES = ['erc20', 'internal']
NATIVE_SYMBOLS = {
    'Ethereum': 'ETH', 'BSC': 'BNB', 'Polygon': 'POL', 'Arbitrum': 'ETH',
    'Optimism': 'ETH', 'Base': 'ETH', 'Avalanche': 'AVAX', 'Blast': 'ETH',
    'HyperEVM': 'HYPE',
}

# Row column order matches the INSERT statement in _insert_rows.
INSERT_COLUMNS = (
    'tx_hash', 'chain', 'protocol', 'timestamp',
    'actual_fee_usd', 'fee_token_symbol', 'fee_token_address', 'fee_amount_raw',
    'swap_volume_usd', 'token_in_symbol', 'token_in_address',
    'token_out_symbol', 'token_out_address',
    'amount_in', 'amount_out',
    'block_number', 'from_address', 'to_address',
    'fee_data_source', 'volume_data_source',
)


def _build_row(
    transfer: Dict,
    source_name: str,
    chain_name: str,
    receiver_address: str,
) -> Optional[Tuple]:
    """Convert one Etherscan tokentx record to a parameter tuple for INSERT.

    Returns None if the transfer should be filtered (no tx hash, or outbound
    — `tokentx?address=X` returns both FROM-X and TO-X transfers; we only
    want fee deposits).
    """
    tx_hash = transfer.get('hash')
    if not tx_hash:
        return None
    to_addr = (transfer.get('to') or '').lower()
    if to_addr != receiver_address.lower():
        return None

    block_number = int(transfer.get('blockNumber') or 0) or None
    timestamp_unix = int(transfer.get('timeStamp') or 0)
    timestamp = (
        datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)
        if timestamp_unix
        else datetime.now(tz=timezone.utc)
    )

    decimals = int(transfer.get('tokenDecimal') or 0)
    raw_wei = int(transfer.get('value') or 0)
    # Decimal arithmetic avoids float precision loss on 18+ decimal tokens.
    human_amount_dec = Decimal(raw_wei) / (Decimal(10) ** decimals) if decimals else Decimal(raw_wei)

    return (
        tx_hash,
        chain_name,
        source_name,
        timestamp,
        0,  # actual_fee_usd — column is NOT NULL; enricher overwrites with the real block-time USD value
        transfer.get('tokenSymbol') or '',
        transfer.get('contractAddress') or '',
        format(human_amount_dec, 'f'),  # fee_amount_raw — full-precision text
        None,  # swap_volume_usd
        None, None, None, None,  # token_in/out_symbol/address
        human_amount_dec,  # amount_in (NUMERIC column accepts Decimal)
        None,  # amount_out
        block_number,
        (transfer.get('from') or '').lower(),
        to_addr,
        'etherscan',
        None,  # volume_data_source
    )


def _build_alchemy_row(
    transfer: Dict,
    source_name: str,
    chain_name: str,
    receiver_address: str,
) -> Optional[Tuple]:
    """Convert one alchemy_getAssetTransfers record to an INSERT tuple.

    Same column layout as _build_row. Native-coin credits ('internal' or
    'external') get the chain's native symbol and an empty token address.
    Returns None for outbound, zero-value or malformed records.
    """
    tx_hash = transfer.get('hash')
    if not tx_hash:
        return None
    to_addr = (transfer.get('to') or '').lower()
    if to_addr != receiver_address.lower():
        return None

    raw = transfer.get('rawContract') or {}
    try:
        raw_value = int(raw.get('value') or '0x0', 16)
        native = transfer.get('category') in ('internal', 'external')
        decimals_hex = raw.get('decimal')
        decimals = int(decimals_hex, 16) if decimals_hex else (18 if native else 0)
        block_number = int(transfer.get('blockNum') or '0x0', 16) or None
    except (TypeError, ValueError):
        return None
    if raw_value == 0:
        return None

    amount = Decimal(raw_value) / (Decimal(10) ** decimals) if decimals else Decimal(raw_value)
    ts_str = (transfer.get('metadata') or {}).get('blockTimestamp')
    timestamp = (
        datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        if ts_str
        else datetime.now(tz=timezone.utc)
    )
    if native:
        symbol = NATIVE_SYMBOLS.get(chain_name) or transfer.get('asset') or ''
        token_address = ''
    else:
        symbol = transfer.get('asset') or ''
        token_address = (raw.get('address') or '').lower()

    return (
        tx_hash,
        chain_name,
        source_name,
        timestamp,
        0,  # actual_fee_usd — enricher prices it
        symbol,
        token_address,
        format(amount, 'f'),
        None,
        None, None, None, None,
        amount,
        None,
        block_number,
        (transfer.get('from') or '').lower(),
        to_addr,
        'etherscan',  # on-chain fee-receiver crawl; consumers filter on this value
        None,
    )


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
        # Shared rate budget: 5 req/s across all chainids on one key.
        self.delay = config.API_DELAYS.get('etherscan', 0.25)
        self.alchemy_key = config.ALCHEMY_API_KEY
        self.alchemy_delay = config.API_DELAYS.get('alchemy', 0.2)
        # full_crawl ignores the cursor (one-off history backfill).
        self.full_crawl = False

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

    def _last_block_for(self, receiver_address: str, chain_name: str) -> Optional[int]:
        """Highest block_number already ingested for (receiver, chain), or
        None to start from chain genesis.

        Keyed on the receiver, not on protocol: the classifier moves rows
        between '1inch', 'kyberswap' and 'other', and a protocol-keyed
        cursor would then step backwards and re-crawl old pages."""
        if self.full_crawl:
            return None
        db = self._get_connection()
        cursor = db.cursor()
        try:
            cursor.execute(
                "SELECT MAX(block_number) FROM dex_aggregator_revenue "
                "WHERE LOWER(to_address) = %s AND chain = %s",
                (receiver_address.lower(), chain_name),
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
        """One page of transfers. Returns (transfers, error_msg). A real API
        failure (HTTP/auth/rate-limit/trial-expired) propagates as a non-None
        error so the orchestrator can write sync_status.last_error — this is
        the regression that hid Arkham's 402 for 7 months."""
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
        message = data.get('message')
        result = data.get('result', [])

        if status == '1' and isinstance(result, list):
            return result, None
        # Etherscan V2 reports "no transactions on this chain" via TWO shapes:
        # `result: []` (older variant) and `message: 'NOTOK'` with `result`
        # set to the generic string 'NOTOK' (current variant on BSC/OP/Base/
        # Avalanche when an address has no txs). Real errors carry a specific
        # message like 'Invalid API Key' or 'Max rate limit reached', so an
        # exact-match check on the empty-signal strings is robust without
        # silently masking auth or rate-limit failures.
        if status == '0' and (result == [] or message in ('No transactions found', 'NOTOK')):
            return [], None
        return [], f"Etherscan chainid={chainid} status={status} msg={message!r} result={result!r}"

    def _insert_rows(self, rows: List[Tuple]) -> int:
        """Batch-insert via execute_values. ON CONFLICT (tx_hash) DO NOTHING
        means returned rowcount equals net-new rows."""
        if not rows:
            return 0
        db = self._get_connection()
        cols = ', '.join(INSERT_COLUMNS)
        sql = (
            f"INSERT INTO dex_aggregator_revenue ({cols}) VALUES %s "
            "ON CONFLICT (tx_hash) DO NOTHING"
        )
        with db.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
            return cur.rowcount

    def fetch_alchemy_page(
        self,
        network: str,
        address: str,
        from_block: int,
        page_key: Optional[str],
        categories: List[str],
    ) -> Tuple[List[Dict], Optional[str], Optional[str]]:
        """One page of inbound transfers. Returns (transfers, next_page_key,
        error_msg); a real failure surfaces as error_msg, like Etherscan."""
        params = {
            'fromBlock': hex(from_block),
            'toBlock': 'latest',
            'toAddress': address,
            'category': categories,
            'withMetadata': True,
            'excludeZeroValue': True,
            'maxCount': hex(ALCHEMY_PAGE_SIZE),
            'order': 'asc',
        }
        if page_key:
            params['pageKey'] = page_key
        body = {'jsonrpc': '2.0', 'id': 1, 'method': 'alchemy_getAssetTransfers', 'params': [params]}
        try:
            r = requests.post(
                f"https://{network}.g.alchemy.com/v2/{self.alchemy_key}", json=body, timeout=30
            )
            r.raise_for_status()
            data = r.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            return [], None, f"Alchemy HTTP error on {network}: {e}"
        if data.get('error'):
            return [], None, f"Alchemy {network} error: {data['error']}"
        result = data.get('result') or {}
        return result.get('transfers') or [], result.get('pageKey'), None

    def _ingest_chain_alchemy(
        self,
        source_name: str,
        receiver_address: str,
        network: str,
        chain_name: str,
    ) -> Tuple[int, Optional[str]]:
        last_block = self._last_block_for(receiver_address, chain_name)
        from_block = (last_block + 1) if last_block else 0
        categories = list(ALCHEMY_CATEGORIES)
        page_key = None
        inserted = 0
        max_pages = None if self.full_crawl else MAX_PAGES_PER_CHAIN

        page = 0
        while max_pages is None or page < max_pages:
            transfers, next_key, err = self.fetch_alchemy_page(
                network, receiver_address, from_block, page_key, categories
            )
            if err and 'internal' in categories:
                # Network without internal-transfer support: ERC-20 only.
                logger.info(f"  {chain_name}: retrying without 'internal' ({err})")
                categories = ['erc20']
                continue
            if err:
                return inserted, err

            rows = [
                row for t in transfers
                if (row := _build_alchemy_row(t, source_name, chain_name, receiver_address)) is not None
            ]
            try:
                page_inserts = self._insert_rows(rows)
                self._get_connection().commit()
            except Exception as e:
                logger.error(f"insert batch failed for {source_name}/{chain_name} page={page + 1}: {e}")
                if self.db:
                    self.db.rollback()
                return inserted, f"{chain_name} insert failed: {e}"

            inserted += page_inserts
            page += 1
            logger.info(
                f"  {source_name}/{chain_name} (alchemy) page={page}: {page_inserts} new "
                f"(of {len(transfers)} returned, {len(rows)} inbound)"
            )
            if not next_key:
                break
            page_key = next_key
            time.sleep(self.alchemy_delay)

        return inserted, None

    def ingest_chain(
        self,
        source_name: str,
        receiver_address: str,
        chainid: int,
        chain_name: str,
    ) -> Tuple[int, Optional[str]]:
        network = config.ALCHEMY_NETWORKS.get(chain_name) if self.alchemy_key else None
        if network:
            return self._ingest_chain_alchemy(source_name, receiver_address, network, chain_name)

        last_block = self._last_block_for(receiver_address, chain_name)
        startblock = (last_block + 1) if last_block else 0
        inserted = 0

        for page in range(1, MAX_PAGES_PER_CHAIN + 1):
            transfers, err = self.fetch_chain_page(chainid, receiver_address, startblock, page)
            if err:
                return inserted, err
            if not transfers:
                break

            rows = [
                row for t in transfers
                if (row := _build_row(t, source_name, chain_name, receiver_address)) is not None
            ]
            try:
                page_inserts = self._insert_rows(rows)
                self._get_connection().commit()
            except Exception as e:
                logger.error(f"insert batch failed for {source_name}/{chain_name} page={page}: {e}")
                if self.db:
                    self.db.rollback()
                return inserted, f"{chain_name} insert failed: {e}"

            inserted += page_inserts
            logger.info(
                f"  {source_name}/{chain_name} page={page}: {page_inserts} new "
                f"(of {len(transfers)} returned, {len(rows)} inbound)"
            )

            if len(transfers) < PAGE_SIZE:
                break
            time.sleep(self.delay)

        return inserted, None

    def ingest_one(self, source_name: str, receiver_address: str) -> Dict:
        result = {'source': source_name, 'inserted': 0, 'error': None}
        errors: List[str] = []
        try:
            self._get_connection()
            for chainid, chain_name in self.chains:
                count, err = self.ingest_chain(source_name, receiver_address, chainid, chain_name)
                result['inserted'] += count
                if err:
                    errors.append(f"{chain_name}: {err}")
                time.sleep(self.delay)
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
    import argparse
    parser = argparse.ArgumentParser(description='Crawl fee-receiver transfers')
    parser.add_argument('--full', action='store_true',
                        help='Ignore the cursor and crawl each chain from genesis (one-off backfill)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ingestor = EtherscanIngestor()
    ingestor.full_crawl = args.full
    for r in ingestor.ingest():
        print(r)


if __name__ == '__main__':
    main()
