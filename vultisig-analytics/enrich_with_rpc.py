#!/usr/bin/env python3
"""
Enrich Arkham records with swap volume data using RPC nodes (Infura/Alchemy).
Fetches transaction data directly from blockchain via JSON-RPC.
"""

import os
import sys
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from dotenv import load_dotenv
import time
import json

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')
INFURA_API_KEY = os.getenv('INFURA_API_KEY')
ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY')

# RPC Endpoints Configuration
# Using public RPCs as fallback when Infura is not available or returns 403
RPC_CONFIG = {
    'Ethereum': {
        'rpc_url': 'https://ethereum.publicnode.com',  # Public RPC
        'fallback_urls': [
            'https://eth.llamarpc.com',
            'https://cloudflare-eth.com',
        ],
        'chain_id': 1,
        'native_symbol': 'ETH',
        'rate_limit': 3,  # Conservative for public RPCs
    },
    'Optimism': {
        'rpc_url': 'https://mainnet.optimism.io',  # Public RPC
        'fallback_urls': [
            'https://optimism.llamarpc.com',
            'https://optimism.publicnode.com',
        ],
        'chain_id': 10,
        'native_symbol': 'ETH',
        'rate_limit': 3,
    },
    'Arbitrum': {
        'rpc_url': 'https://arb1.arbitrum.io/rpc',  # Public RPC
        'fallback_urls': [
            'https://arbitrum.llamarpc.com',
            'https://arbitrum.publicnode.com',
        ],
        'chain_id': 42161,
        'native_symbol': 'ETH',
        'rate_limit': 3,
    },
    'Polygon': {
        'rpc_url': 'https://polygon-rpc.com',  # Public RPC
        'fallback_urls': [
            'https://polygon.llamarpc.com',
            'https://polygon.publicnode.com',
        ],
        'chain_id': 137,
        'native_symbol': 'MATIC',
        'rate_limit': 3,
    },
    'Base': {
        'rpc_url': 'https://mainnet.base.org',  # Public RPC
        'fallback_urls': [
            'https://base.llamarpc.com',
            'https://base.publicnode.com',
        ],
        'chain_id': 8453,
        'native_symbol': 'ETH',
        'rate_limit': 3,
    },
    'Avalanche': {
        'rpc_url': 'https://api.avax.network/ext/bc/C/rpc',  # Public RPC
        'fallback_urls': [
            'https://avalanche.publicnode.com',
        ],
        'chain_id': 43114,
        'native_symbol': 'AVAX',
        'rate_limit': 3,
    },
    'BSC': {
        'rpc_url': 'https://bsc-dataseed.binance.org',  # Public RPC
        'fallback_urls': [
            'https://binance.llamarpc.com',
            'https://bsc.publicnode.com',
        ],
        'chain_id': 56,
        'native_symbol': 'BNB',
        'rate_limit': 3,
    },
}


class RPCEnricher:
    def __init__(self):
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL not set")

        self.db = psycopg2.connect(DATABASE_URL)
        self.last_request_time = {}

    def check_rpc_endpoints(self):
        """Check which RPC endpoints are configured."""
        logger.info("="*80)
        logger.info("RPC ENDPOINT STATUS")
        logger.info("="*80)

        available_chains = []
        missing_chains = []

        # Check for Infura/Alchemy keys
        if INFURA_API_KEY:
            logger.info(f"✅ Infura API Key: {INFURA_API_KEY[:10]}...{INFURA_API_KEY[-6:]}")
        else:
            logger.warning("❌ INFURA_API_KEY not set")
            logger.warning("   Get one at: https://infura.io")

        if ALCHEMY_API_KEY:
            logger.info(f"✅ Alchemy API Key: {ALCHEMY_API_KEY[:10]}...{ALCHEMY_API_KEY[-6:]}")
        else:
            logger.info("ℹ️  ALCHEMY_API_KEY not set (optional)")

        logger.info("\nRPC Endpoints:")
        for chain, config in RPC_CONFIG.items():
            if config['rpc_url']:
                # Mask API key in URL for display
                display_url = config['rpc_url']
                if INFURA_API_KEY and INFURA_API_KEY in display_url:
                    display_url = display_url.replace(INFURA_API_KEY, "***")
                logger.info(f"✅ {chain}: {display_url}")
                available_chains.append(chain)
            else:
                logger.warning(f"❌ {chain}: No RPC endpoint")
                missing_chains.append(chain)

        logger.info("="*80)

        if not available_chains:
            logger.error("No RPC endpoints configured!")
            return False

        logger.info(f"\nCan enrich {len(available_chains)} chains: {', '.join(available_chains)}")
        return True

    def rate_limit_wait(self, chain: str):
        """Respect rate limits for RPC calls."""
        config = RPC_CONFIG.get(chain)
        if not config:
            return

        rate_limit = config['rate_limit']
        min_interval = 1.0 / rate_limit

        if chain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[chain]
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                time.sleep(wait_time)

        self.last_request_time[chain] = time.time()

    def fetch_transaction_rpc(self, tx_hash: str, chain: str) -> dict:
        """
        Fetch transaction using JSON-RPC eth_getTransactionByHash.

        Returns:
            {
                'input': '0x...',
                'value': '0x...',
                'from': '0x...',
                'to': '0x...',
                'gas': '0x...',
                ...
            }
        """
        config = RPC_CONFIG.get(chain)
        if not config or not config['rpc_url']:
            logger.warning(f"No RPC endpoint for {chain}")
            return None

        self.rate_limit_wait(chain)

        payload = {
            'jsonrpc': '2.0',
            'method': 'eth_getTransactionByHash',
            'params': [tx_hash],
            'id': 1
        }

        try:
            response = requests.post(
                config['rpc_url'],
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            if 'error' in data:
                logger.error(f"RPC error for {tx_hash}: {data['error']}")
                return None

            if 'result' in data and data['result']:
                return data['result']

            logger.warning(f"No result for {tx_hash} on {chain}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {tx_hash} from {chain} RPC: {e}")
            return None

    def parse_1inch_swap(self, tx_data: dict, chain: str) -> dict:
        """
        Parse 1inch swap transaction to extract volume.

        Returns:
            {
                'amount_in': float,
                'token_in_address': str,
                'token_in_symbol': str (if native),
                'amount_in_wei': str
            }
        """
        input_data = tx_data.get('input', '')
        value_hex = tx_data.get('value', '0x0')

        # Convert hex value to int
        value_wei = int(value_hex, 16) if value_hex else 0

        # Check if native token swap (ETH/BNB/MATIC/AVAX)
        if value_wei > 0:
            native_amount = value_wei / 1e18

            return {
                'amount_in': native_amount,
                'amount_in_wei': str(value_wei),
                'token_in_address': 'NATIVE',
                'token_in_symbol': RPC_CONFIG[chain]['native_symbol'],
                'parsing_method': 'native_value'
            }

        # Parse ERC20 swap from input data
        if len(input_data) < 10:
            logger.warning(f"Input data too short: {len(input_data)} chars")
            return None

        func_sig = input_data[:10]

        # Try to parse with generic method (works for most 1inch functions)
        # Most 1inch functions have amount as one of the early parameters
        return self.parse_1inch_generic(input_data, func_sig, chain)

    def parse_1inch_v5_swap(self, input_data: str, chain: str) -> dict:
        """
        Parse 1inch v5 swap function.

        The swap amount is in the SwapDescription struct at a predictable offset.
        We'll extract it heuristically by finding large uint256 values.
        """
        try:
            data = input_data[10:]  # Skip 0x + function sig (8 chars)

            # Look for amount fields (large uint256 values)
            # Each parameter is 32 bytes = 64 hex chars
            potential_amounts = []

            for i in range(2, min(15, len(data)//64)):  # Check positions 2-14
                chunk = data[i*64:(i+1)*64]
                if len(chunk) == 64:
                    try:
                        value = int(chunk, 16)
                        # Filter for reasonable token amounts
                        # Min: 1000 wei, Max: 10^30 (avoid addresses/flags)
                        if 1000 < value < 10**30:
                            potential_amounts.append((i, value, chunk))
                    except:
                        pass

            if potential_amounts:
                # First large value is typically srcAmount
                _, amount_wei, _ = potential_amounts[0]

                # Try to extract srcToken address (usually before amount)
                # Addresses are 20 bytes, padded to 32 bytes (last 40 hex chars)
                token_address = None
                for i in range(3, 8):  # Check a few positions
                    chunk = data[i*64:(i+1)*64]
                    if len(chunk) == 64:
                        addr = '0x' + chunk[-40:]
                        # Basic validation: address should start with reasonable chars
                        if addr.startswith('0x') and len(addr) == 42:
                            token_address = addr
                            break

                return {
                    'amount_in_wei': str(amount_wei),
                    'amount_in': amount_wei / 1e18,  # Assume 18 decimals
                    'token_in_address': token_address,
                    'parsing_method': '1inch_v5_heuristic'
                }

            return None

        except Exception as e:
            logger.error(f"Error parsing 1inch v5 swap: {e}")
            return None

    def parse_1inch_unoswap(self, input_data: str, chain: str) -> dict:
        """Parse unoswap function (simpler)."""
        try:
            data = input_data[10:]

            # unoswap params: srcToken (32 bytes), amount (32 bytes), ...
            if len(data) >= 128:
                # Token at position 0
                token_chunk = data[0:64]
                token_address = '0x' + token_chunk[-40:]

                # Amount at position 1
                amount_hex = data[64:128]
                amount_wei = int(amount_hex, 16)

                return {
                    'amount_in_wei': str(amount_wei),
                    'amount_in': amount_wei / 1e18,
                    'token_in_address': token_address,
                    'parsing_method': '1inch_unoswap'
                }

            return None

        except Exception as e:
            logger.error(f"Error parsing unoswap: {e}")
            return None

    def parse_1inch_generic(self, input_data: str, func_sig: str, chain: str) -> dict:
        """
        Generic parser that works for most 1inch functions.
        Extracts the largest uint256 value which is usually the swap amount.
        """
        try:
            data = input_data[10:]  # Skip function signature

            # Find all potential amounts (large uint256 values)
            amounts = []
            tokens = []

            # Scan through parameters
            for i in range(0, min(20, len(data)//64)):
                chunk = data[i*64:(i+1)*64]
                if len(chunk) == 64:
                    try:
                        value = int(chunk, 16)

                        # Check if it's an amount (reasonable range)
                        if 1000 < value < 10**30:
                            amounts.append((i, value))

                        # Check if it's an address (20 bytes, padded)
                        if value < 2**160:  # Addresses are 160 bits
                            addr = '0x' + chunk[-40:]
                            # Basic address validation
                            if all(c in '0123456789abcdef' for c in addr[2:].lower()):
                                tokens.append((i, addr))
                    except:
                        pass

            if amounts:
                # Use the first or largest amount found
                # Usually the swap amount is one of the earlier large values
                _, amount_wei = amounts[0]

                # Try to find a token address
                token_address = None
                if tokens:
                    # Use first reasonable token address
                    _, token_address = tokens[0]

                return {
                    'amount_in_wei': str(amount_wei),
                    'amount_in': amount_wei / 1e18,  # Assume 18 decimals
                    'token_in_address': token_address,
                    'parsing_method': f'generic_{func_sig}'
                }

            logger.warning(f"No amounts found in transaction data")
            return None

        except Exception as e:
            logger.error(f"Error in generic parser: {e}")
            return None

    def get_token_price_usd(self, token_symbol: str, chain: str, timestamp: datetime) -> float:
        """Get token price from historical_prices table."""
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT price_usd
            FROM historical_prices
            WHERE token_symbol = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (token_symbol, timestamp))

        result = cursor.fetchone()
        return float(result['price_usd']) if result else 0.0

    def enrich_record(self, record: dict) -> bool:
        """Enrich a single record with volume from RPC."""
        tx_hash = record['tx_hash']
        chain = record['chain']
        timestamp = record['timestamp']

        logger.info(f"Enriching {tx_hash[:16]}... on {chain}")

        # Fetch transaction via RPC
        tx_data = self.fetch_transaction_rpc(tx_hash, chain)
        if not tx_data:
            return False

        # Parse swap data
        parsed = self.parse_1inch_swap(tx_data, chain)
        if not parsed:
            logger.warning(f"Could not parse swap data")
            logger.debug(f"TX input: {tx_data.get('input', '')[:100]}...")
            return False

        # Get token info
        token_address = parsed.get('token_in_address')
        token_symbol = parsed.get('token_in_symbol')
        amount_in = parsed.get('amount_in', 0)

        # Get price
        price_usd = 0.0
        if token_symbol:
            price_usd = self.get_token_price_usd(token_symbol, chain, timestamp)

        # Calculate volume USD
        swap_volume_usd = amount_in * price_usd if price_usd > 0 else None

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
            amount_in,
            tx_hash
        ))

        vol_str = f"${swap_volume_usd:.2f}" if swap_volume_usd else "N/A"
        logger.info(f"✓ {amount_in:.6f} {token_symbol or 'TOKEN'} = {vol_str} ({parsed.get('parsing_method')})")
        return True

    def enrich_missing_volumes(self, limit: int = None):
        """Enrich all records missing volume data."""
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        # Get chains we have RPC endpoints for
        available_chains = [chain for chain, cfg in RPC_CONFIG.items() if cfg['rpc_url']]

        if not available_chains:
            logger.error("No RPC endpoints available!")
            return

        placeholders = ','.join(['%s'] * len(available_chains))
        query = f"""
            SELECT tx_hash, chain, timestamp, actual_fee_usd, protocol
            FROM dex_aggregator_revenue
            WHERE swap_volume_usd IS NULL
              AND fee_data_source = 'arkham'
              AND protocol = '1inch'
              AND chain IN ({placeholders})
            ORDER BY timestamp DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query, available_chains)
        records = cursor.fetchall()

        if not records:
            logger.info("No records to enrich!")
            return

        logger.info(f"Found {len(records)} records to enrich\n")

        enriched_count = 0
        failed_count = 0

        for i, record in enumerate(records, 1):
            logger.info(f"[{i}/{len(records)}] {record['chain']}")

            try:
                success = self.enrich_record(record)

                if success:
                    enriched_count += 1
                else:
                    failed_count += 1

                # Commit every 10 records
                if i % 10 == 0:
                    self.db.commit()
                    logger.info(f"Progress: {enriched_count} enriched, {failed_count} failed\n")

            except Exception as e:
                logger.error(f"Error: {e}")
                import traceback
                traceback.print_exc()
                failed_count += 1
                self.db.rollback()

        # Final commit
        self.db.commit()

        logger.info(f"\n{'='*80}")
        logger.info("ENRICHMENT COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Total: {len(records)}, Enriched: {enriched_count}, Failed: {failed_count}")
        if len(records) > 0:
            logger.info(f"Success rate: {enriched_count/len(records)*100:.1f}%")

    def close(self):
        if self.db:
            self.db.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Enrich Arkham records using RPC nodes')
    parser.add_argument('--check', action='store_true', help='Check RPC endpoint status')
    parser.add_argument('--test', action='store_true', help='Test mode: only 5 records')
    parser.add_argument('--limit', type=int, help='Limit number of records')

    args = parser.parse_args()

    try:
        enricher = RPCEnricher()

        # Check RPC status
        if not enricher.check_rpc_endpoints():
            if args.check:
                sys.exit(0)
            else:
                logger.error("\nCannot proceed without RPC endpoints!")
                sys.exit(1)

        if args.check:
            sys.exit(0)

        # Run enrichment
        if args.test:
            logger.info("\nTEST MODE: Enriching 5 records\n")
            enricher.enrich_missing_volumes(limit=5)
        else:
            enricher.enrich_missing_volumes(limit=args.limit)

        enricher.close()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
