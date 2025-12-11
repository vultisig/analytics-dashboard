#!/usr/bin/env python3
"""
Enhanced RPC-based volume enrichment with:
1. Token decimal lookup from blockchain/database
2. Chain-specific adjustments
3. Better logging with explorer links
4. Fixed historical_prices query
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

# RPC Endpoints
RPC_CONFIG = {
    'Ethereum': {'rpc_url': 'https://ethereum.publicnode.com', 'native_symbol': 'ETH'},
    'Optimism': {'rpc_url': 'https://mainnet.optimism.io', 'native_symbol': 'ETH'},
    'Arbitrum': {'rpc_url': 'https://arb1.arbitrum.io/rpc', 'native_symbol': 'ETH'},
    'Base': {'rpc_url': 'https://mainnet.base.org', 'native_symbol': 'ETH'},
    'BSC': {'rpc_url': 'https://bsc-dataseed.binance.org', 'native_symbol': 'BNB'},
    'Polygon': {'rpc_url': 'https://polygon-rpc.com', 'native_symbol': 'MATIC'},
    'Avalanche': {'rpc_url': 'https://api.avax.network/ext/bc/C/rpc', 'native_symbol': 'AVAX'},
}

# Block Explorers
EXPLORERS = {
    'Ethereum': 'https://etherscan.io/tx/',
    'Optimism': 'https://optimistic.etherscan.io/tx/',
    'Arbitrum': 'https://arbiscan.io/tx/',
    'Base': 'https://basescan.org/tx/',
    'BSC': 'https://bscscan.com/tx/',
    'Polygon': 'https://polygonscan.com/tx/',
    'Avalanche': 'https://snowtrace.io/tx/'
}

# Common token addresses with known decimals
KNOWN_TOKENS = {
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': {'symbol': 'USDC', 'decimals': 6},  # Ethereum USDC
    '0x0b2c639c533813f4aa9d7837caf62653d097ff85': {'symbol': 'USDC', 'decimals': 6},  # Optimism USDC
    '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8': {'symbol': 'USDC', 'decimals': 6},  # Arbitrum USDC (bridged)
    '0xaf88d065e77c8cc2239327c5edb3a432268e5831': {'symbol': 'USDC', 'decimals': 6},  # Arbitrum USDC (native)
    '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913': {'symbol': 'USDC', 'decimals': 6},  # Base USDC
    '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d': {'symbol': 'USDC', 'decimals': 18}, # BSC USDC
    '0x55d398326f99059ff775485246999027b3197955': {'symbol': 'USDT', 'decimals': 18}, # BSC USDT
}


class ImprovedRPCEnricher:
    def __init__(self):
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL not set")
        self.db = psycopg2.connect(DATABASE_URL)
        self.rate_limit = 3  # requests per second

    def fetch_transaction_rpc(self, tx_hash: str, chain: str) -> dict:
        """Fetch transaction data from RPC."""
        config = RPC_CONFIG.get(chain)
        if not config:
            logger.error(f"No RPC config for chain: {chain}")
            return None

        rpc_url = config['rpc_url']
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 1
        }

        try:
            response = requests.post(rpc_url, json=payload, timeout=10)
            result = response.json()

            if 'result' in result and result['result']:
                return result['result']
            else:
                logger.warning(f"Transaction not found: {tx_hash}")
                return None

        except Exception as e:
            logger.error(f"RPC error for {chain}: {e}")
            return None

    def get_token_decimals(self, token_address: str, chain: str) -> int:
        """Get token decimals from known list, database, or blockchain."""
        if not token_address:
            return 18  # Default for native tokens

        # Check known tokens
        token_lower = token_address.lower()
        if token_lower in KNOWN_TOKENS:
            return KNOWN_TOKENS[token_lower]['decimals']

        # Check asset_decimals table
        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT decimal_places
            FROM asset_decimals
            WHERE LOWER(contract_address) = %s OR full_asset_id LIKE %s
            LIMIT 1
        """, (token_lower, f"%{token_address}%"))

        result = cursor.fetchone()
        cursor.close()

        if result:
            logger.debug(f"Found decimals in DB: {token_address} = {result['decimal_places']}")
            return result['decimal_places']

        # Try to fetch from blockchain
        decimals = self.fetch_decimals_from_blockchain(token_address, chain)
        if decimals:
            # Cache it
            cursor = self.db.cursor()
            cursor.execute("""
                INSERT INTO asset_decimals (asset_symbol, chain, decimal_places, contract_address)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (full_asset_id) DO NOTHING
            """, ('UNKNOWN', chain, decimals, token_address))
            self.db.commit()
            cursor.close()
            return decimals

        # Default to 18
        logger.warning(f"Could not determine decimals for {token_address}, defaulting to 18")
        return 18

    def fetch_decimals_from_blockchain(self, token_address: str, chain: str) -> int:
        """Fetch token decimals from blockchain using RPC."""
        config = RPC_CONFIG.get(chain)
        if not config:
            return None

        # ERC20 decimals() function selector
        data = '0x313ce567'

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "to": token_address,
                "data": data
            }, "latest"],
            "id": 1
        }

        try:
            response = requests.post(config['rpc_url'], json=payload, timeout=10)
            result = response.json()

            if 'result' in result and result['result'] != '0x':
                decimals = int(result['result'], 16)
                logger.info(f"Fetched decimals from blockchain: {token_address} = {decimals}")
                return decimals

        except Exception as e:
            logger.debug(f"Could not fetch decimals from blockchain: {e}")

        return None

    def parse_1inch_generic(self, input_data: str, func_sig: str) -> dict:
        """Generic parser for 1inch transactions."""
        try:
            data = input_data[10:]  # Skip function signature
            amounts = []
            tokens = []

            # Scan parameters
            for i in range(0, min(20, len(data)//64)):
                chunk = data[i*64:(i+1)*64]
                if len(chunk) == 64:
                    try:
                        value = int(chunk, 16)

                        # Check if it's an amount
                        if 1000 < value < 10**30:
                            amounts.append((i, value))

                        # Check if it's an address
                        if value < 2**160:
                            addr = '0x' + chunk[-40:]
                            if all(c in '0123456789abcdef' for c in addr[2:].lower()):
                                tokens.append((i, addr))
                    except:
                        pass

            if amounts:
                _, amount_wei = amounts[0]
                token_address = tokens[0][1] if tokens else None

                return {
                    'amount_in_wei': str(amount_wei),
                    'token_in_address': token_address,
                    'parsing_method': f'generic_{func_sig}'
                }

            return None

        except Exception as e:
            logger.error(f"Error in generic parser: {e}")
            return None

    def enrich_record(self, record: dict) -> bool:
        """Enrich a single record with volume from RPC."""
        tx_hash = record['tx_hash']
        chain = record['chain']
        timestamp = record['timestamp']
        fee_usd = float(record['actual_fee_usd']) if record['actual_fee_usd'] else 0

        explorer_url = EXPLORERS.get(chain, '') + tx_hash
        logger.info(f"Enriching {chain} tx: {tx_hash[:16]}... (Fee: ${fee_usd:.2f})")
        logger.info(f"  Explorer: {explorer_url}")

        # Fetch transaction
        tx_data = self.fetch_transaction_rpc(tx_hash, chain)
        if not tx_data:
            logger.error(f"  ❌ Could not fetch transaction")
            return False

        input_data = tx_data.get('input', '')
        if len(input_data) < 10:
            logger.error(f"  ❌ Input data too short")
            return False

        func_sig = input_data[:10]

        # Parse swap data
        parsed = self.parse_1inch_generic(input_data, func_sig)
        if not parsed:
            logger.error(f"  ❌ Could not parse swap data")
            return False

        # Get token info
        token_address = parsed.get('token_in_address')
        amount_wei = int(parsed.get('amount_in_wei', 0))

        # Get decimals (chain-specific)
        decimals = self.get_token_decimals(token_address, chain)
        amount_tokens = amount_wei / (10 ** decimals)

        # Get token symbol
        token_symbol = None
        if token_address:
            token_lower = token_address.lower()
            if token_lower in KNOWN_TOKENS:
                token_symbol = KNOWN_TOKENS[token_lower]['symbol']
            else:
                # Try to fetch from database
                cursor = self.db.cursor(cursor_factory=RealDictCursor)
                cursor.execute("""
                    SELECT asset_symbol
                    FROM asset_decimals
                    WHERE LOWER(contract_address) = %s
                    LIMIT 1
                """, (token_lower,))
                result = cursor.fetchone()
                cursor.close()
                if result:
                    token_symbol = result['asset_symbol']

        # Get price (fix column name)
        price_usd = 0.0
        if token_symbol:
            cursor = self.db.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT price_usd
                FROM historical_prices
                WHERE symbol = %s
                  AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (token_symbol, timestamp))
            result = cursor.fetchone()
            cursor.close()
            if result:
                price_usd = float(result['price_usd'])

        # Calculate volume USD
        swap_volume_usd = amount_tokens * price_usd if price_usd > 0 else None

        # Log results
        logger.info(f"  ✅ Parsed: {amount_tokens:.6f} tokens ({decimals} decimals)")
        logger.info(f"     Token: {token_symbol or 'UNKNOWN'} ({token_address or 'Native'})")
        if swap_volume_usd:
            logger.info(f"     Volume: ${swap_volume_usd:.2f} (@ ${price_usd:.4f}/token)")
        else:
            logger.info(f"     Volume: N/A (no price data)")

        # Update database
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE dex_aggregator_revenue
            SET
                swap_volume_usd = %s,
                token_in_symbol = %s,
                token_in_address = %s,
                amount_in = %s,
                volume_data_source = 'rpc',
                updated_at = NOW()
            WHERE tx_hash = %s
        """, (
            swap_volume_usd,
            token_symbol,
            token_address,
            amount_tokens,
            tx_hash
        ))
        self.db.commit()
        cursor.close()

        return True

    def enrich_all_failed(self, limit: int = None):
        """Enrich all failed transactions."""
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        available_chains = list(RPC_CONFIG.keys())
        placeholders = ','.join(['%s'] * len(available_chains))

        query = f"""
            SELECT tx_hash, chain, timestamp, actual_fee_usd, protocol
            FROM dex_aggregator_revenue
            WHERE swap_volume_usd IS NULL
              AND protocol = '1inch'
              AND chain IN ({placeholders})
            ORDER BY timestamp DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query, tuple(available_chains))
        records = cursor.fetchall()
        cursor.close()

        logger.info(f"Found {len(records)} records to enrich")
        logger.info("=" * 100)

        enriched = 0
        failed = 0

        for i, record in enumerate(records, 1):
            logger.info(f"\n[{i}/{len(records)}] Processing...")

            try:
                if self.enrich_record(record):
                    enriched += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"  ❌ Exception: {e}")
                failed += 1

            # Rate limit
            time.sleep(1.0 / self.rate_limit)

        logger.info("\n" + "=" * 100)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("=" * 100)
        logger.info(f"Total: {len(records)}, Enriched: {enriched}, Failed: {failed}")
        logger.info(f"Success rate: {enriched/len(records)*100:.1f}%")

        return enriched, failed


def main():
    enricher = ImprovedRPCEnricher()

    # Enrich all failed transactions
    enricher.enrich_all_failed()


if __name__ == '__main__':
    main()
