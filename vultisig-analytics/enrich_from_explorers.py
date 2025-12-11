#!/usr/bin/env python3
"""
Enrich Arkham records with swap volume data from block explorer APIs.
Uses Etherscan, BscScan, BaseScan, Arbiscan, etc. to fetch transaction details.
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

# Block Explorer API Configuration
EXPLORER_CONFIG = {
    'Ethereum': {
        'api_url': 'https://api.etherscan.io/api',
        'api_key_env': 'ETHERSCAN_API_KEY',
        'api_key': os.getenv('ETHERSCAN_API_KEY'),
        'rate_limit': 5,  # requests per second
        'signup_url': 'https://etherscan.io/apis'
    },
    'BSC': {
        'api_url': 'https://api.bscscan.com/api',
        'api_key_env': 'BSCSCAN_API_KEY',
        'api_key': os.getenv('BSCSCAN_API_KEY'),
        'rate_limit': 5,
        'signup_url': 'https://bscscan.com/apis'
    },
    'Base': {
        'api_url': 'https://api.basescan.org/api',
        'api_key_env': 'BASESCAN_API_KEY',
        'api_key': os.getenv('BASESCAN_API_KEY') or os.getenv('ETHERSCAN_API_KEY'),  # Can reuse Etherscan key
        'rate_limit': 5,
        'signup_url': 'https://basescan.org/apis'
    },
    'Optimism': {
        'api_url': 'https://api-optimistic.etherscan.io/api',
        'api_key_env': 'OPTIMISM_API_KEY',
        'api_key': os.getenv('OPTIMISM_API_KEY') or os.getenv('ETHERSCAN_API_KEY'),
        'rate_limit': 5,
        'signup_url': 'https://optimistic.etherscan.io/apis'
    },
    'Arbitrum': {
        'api_url': 'https://api.arbiscan.io/api',
        'api_key_env': 'ARBISCAN_API_KEY',
        'api_key': os.getenv('ARBISCAN_API_KEY') or os.getenv('ETHERSCAN_API_KEY'),
        'rate_limit': 5,
        'signup_url': 'https://arbiscan.io/apis'
    },
    'Polygon': {
        'api_url': 'https://api.polygonscan.com/api',
        'api_key_env': 'POLYGONSCAN_API_KEY',
        'api_key': os.getenv('POLYGONSCAN_API_KEY'),
        'rate_limit': 5,
        'signup_url': 'https://polygonscan.com/apis'
    },
    'Avalanche': {
        'api_url': 'https://api.snowtrace.io/api',
        'api_key_env': 'SNOWTRACE_API_KEY',
        'api_key': os.getenv('SNOWTRACE_API_KEY'),
        'rate_limit': 5,
        'signup_url': 'https://snowtrace.io/apis'
    },
}

# 1inch Router contract addresses (for identifying 1inch swaps)
ONEINCH_ROUTERS = {
    'v5': '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch v5 Router
    'v4': '0x1111111254fb6c44bac0bed2854e76f90643097d',
}

# Known 1inch function signatures
ONEINCH_FUNCTIONS = {
    '0x12aa3caf': 'swap',  # swap(address,(address,address,address,address,uint256,uint256,uint256),bytes,bytes)
    '0xe449022e': 'uniswapV3Swap',
    '0x2e95b6c8': 'unoswap',
}


class ExplorerEnricher:
    def __init__(self):
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL not set")

        self.db = psycopg2.connect(DATABASE_URL)
        self.last_request_time = {}  # Track last request time per chain for rate limiting

    def check_api_keys(self):
        """Check which API keys are configured and provide signup URLs for missing ones."""
        logger.info("="*80)
        logger.info("BLOCK EXPLORER API KEY STATUS")
        logger.info("="*80)

        available_chains = []
        missing_chains = []

        for chain, config in EXPLORER_CONFIG.items():
            if config['api_key']:
                logger.info(f"✅ {chain}: API key found ({config['api_key_env']})")
                available_chains.append(chain)
            else:
                logger.warning(f"❌ {chain}: No API key ({config['api_key_env']})")
                logger.warning(f"   Get one at: {config['signup_url']}")
                missing_chains.append(chain)

        logger.info("="*80)
        if not available_chains:
            logger.error("No block explorer API keys configured!")
            logger.info("\nTo get API keys, visit:")
            for chain in missing_chains:
                logger.info(f"  {chain}: {EXPLORER_CONFIG[chain]['signup_url']}")
            logger.info("\nAdd them to your .env file:")
            for chain in missing_chains:
                logger.info(f"  {EXPLORER_CONFIG[chain]['api_key_env']}=your_key_here")
            return False

        logger.info(f"\nCan enrich {len(available_chains)} chains: {', '.join(available_chains)}")
        return True

    def rate_limit_wait(self, chain: str):
        """Respect rate limits for block explorer APIs."""
        config = EXPLORER_CONFIG.get(chain)
        if not config:
            return

        rate_limit = config['rate_limit']  # requests per second
        min_interval = 1.0 / rate_limit

        if chain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[chain]
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s for {chain}")
                time.sleep(wait_time)

        self.last_request_time[chain] = time.time()

    def fetch_transaction(self, tx_hash: str, chain: str) -> dict:
        """
        Fetch transaction details from block explorer API.

        Returns:
            {
                'input': '0x...',  # Transaction input data
                'value': '0',  # ETH/native value
                'from': '0x...',
                'to': '0x...',
                'gasUsed': '...',
                'status': '1'
            }
        """
        config = EXPLORER_CONFIG.get(chain)
        if not config or not config['api_key']:
            logger.warning(f"No API key for {chain}")
            return None

        # Rate limiting
        self.rate_limit_wait(chain)

        # API request
        params = {
            'module': 'proxy',
            'action': 'eth_getTransactionByHash',
            'txhash': tx_hash,
            'apikey': config['api_key']
        }

        try:
            response = requests.get(config['api_url'], params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if data.get('status') == '0' and 'rate limit' in data.get('message', '').lower():
                logger.error(f"Rate limit hit for {chain} - consider increasing wait time")
                return None

            if data.get('result'):
                return data['result']

            logger.warning(f"No result for {tx_hash}: {data.get('message')}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {tx_hash} from {chain}: {e}")
            return None

    def parse_1inch_swap(self, tx_data: dict, chain: str) -> dict:
        """
        Parse 1inch swap transaction to extract volume.

        Returns:
            {
                'amount_in': float,
                'token_in_address': str,
                'swap_volume_usd': float (if price available)
            }
        """
        input_data = tx_data.get('input', '')
        value = tx_data.get('value', '0')

        # Check if it's native token swap (ETH/BNB/etc)
        if value and value != '0' and int(value, 16) > 0:
            native_amount_wei = int(value, 16)
            native_amount = native_amount_wei / 1e18

            return {
                'amount_in': native_amount,
                'token_in_address': 'NATIVE',
                'token_in_symbol': self.get_native_symbol(chain),
                'amount_in_wei': str(native_amount_wei)
            }

        # Parse input data for ERC20 swaps
        if len(input_data) < 10:
            logger.warning("Input data too short")
            return None

        # Extract function signature (first 4 bytes / 8 hex chars + 0x)
        func_sig = input_data[:10]

        if func_sig == '0x12aa3caf':  # swap(address,(address,address,address,address,uint256,uint256,uint256),bytes,bytes)
            return self.parse_1inch_v5_swap(input_data)
        elif func_sig == '0x2e95b6c8':  # unoswap
            return self.parse_1inch_unoswap(input_data)
        else:
            logger.debug(f"Unknown function signature: {func_sig}")
            return None

    def parse_1inch_v5_swap(self, input_data: str) -> dict:
        """
        Parse 1inch v5 swap function.
        Function: swap(address executor, SwapDescription desc, bytes permit, bytes data)

        SwapDescription struct:
          - srcToken (address) - offset 0x04
          - dstToken (address) - offset 0x24
          - srcReceiver (address) - offset 0x44
          - dstReceiver (address) - offset 0x64
          - amount (uint256) - offset 0x84  ← THIS IS THE SWAP VOLUME!
          - minReturnAmount (uint256) - offset 0xa4
          - flags (uint256) - offset 0xc4
        """
        try:
            # Skip function selector (0x + 8 chars)
            data = input_data[10:]

            # Parse parameters (each is 32 bytes = 64 hex chars)
            # First parameter: executor (ignore)
            # Second parameter: tuple offset (ignore)

            # The actual struct starts at a dynamic offset
            # For simplicity, we'll extract common positions

            # Token addresses are typically at predictable offsets
            # srcToken at position 4 (after executor + desc_offset + tuple_marker + srcToken_offset)
            # amount at position after addresses

            # Simple extraction (may need adjustment based on actual encoding)
            # Position in hex: 0x04 (func) + 0x40 (params) + struct offset

            # Try to find amount (uint256) - typically around position 5-7
            # This is a heuristic approach
            tokens_and_amounts = []
            for i in range(2, min(10, len(data)//64)):
                chunk = data[i*64:(i+1)*64]
                if len(chunk) == 64:
                    try:
                        value = int(chunk, 16)
                        # Filter out values that look like amounts (not addresses)
                        if value > 1000 and value < 10**30:  # Reasonable token amount range
                            tokens_and_amounts.append((i, value))
                    except:
                        pass

            if tokens_and_amounts:
                # Typically the first large value is the srcAmount
                amount_wei = tokens_and_amounts[0][1]

                # Try to extract srcToken address (usually at offset 4-5)
                src_token_chunk = data[4*64:5*64]  # Heuristic position
                if len(src_token_chunk) == 64:
                    # Address is last 40 hex chars (20 bytes)
                    src_token = '0x' + src_token_chunk[-40:]
                else:
                    src_token = None

                return {
                    'amount_in_wei': str(amount_wei),
                    'amount_in': amount_wei / 1e18,  # Assume 18 decimals for now
                    'token_in_address': src_token,
                    'parsing_method': '1inch_v5_heuristic'
                }

            return None

        except Exception as e:
            logger.error(f"Error parsing 1inch v5 swap: {e}")
            return None

    def parse_1inch_unoswap(self, input_data: str) -> dict:
        """Parse 1inch unoswap function (simpler single-pool swap)."""
        try:
            data = input_data[10:]

            # unoswap params: srcToken, amount, minReturn, pools
            # Amount is at position 1 (64 hex chars offset)
            if len(data) >= 128:
                amount_hex = data[64:128]
                amount_wei = int(amount_hex, 16)

                # srcToken at position 0
                token_hex = data[0:64]
                src_token = '0x' + token_hex[-40:]

                return {
                    'amount_in_wei': str(amount_wei),
                    'amount_in': amount_wei / 1e18,
                    'token_in_address': src_token,
                    'parsing_method': '1inch_unoswap'
                }

            return None

        except Exception as e:
            logger.error(f"Error parsing unoswap: {e}")
            return None

    def get_native_symbol(self, chain: str) -> str:
        """Get native token symbol for chain."""
        symbols = {
            'Ethereum': 'ETH',
            'BSC': 'BNB',
            'Polygon': 'MATIC',
            'Arbitrum': 'ETH',
            'Optimism': 'ETH',
            'Base': 'ETH',
            'Avalanche': 'AVAX',
            'Blast': 'ETH'
        }
        return symbols.get(chain, 'ETH')

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
        """Enrich a single Arkham record with volume from block explorer."""
        tx_hash = record['tx_hash']
        chain = record['chain']
        timestamp = record['timestamp']

        logger.info(f"Enriching {tx_hash} on {chain}")

        # Fetch transaction
        tx_data = self.fetch_transaction(tx_hash, chain)
        if not tx_data:
            return False

        # Parse swap data
        parsed = self.parse_1inch_swap(tx_data, chain)
        if not parsed:
            logger.warning(f"Could not parse swap data for {tx_hash}")
            logger.debug(f"TX input: {tx_data.get('input', '')[:200]}...")
            return False

        # Get token info
        token_address = parsed.get('token_in_address')
        token_symbol = parsed.get('token_in_symbol')

        # If no symbol, try to look it up (would need additional API calls or database)
        if not token_symbol and token_address and token_address != 'NATIVE':
            # For now, we'll skip price lookup without symbol
            token_symbol = f"TOKEN_{token_address[:8]}"

        # Get price
        price_usd = 0.0
        if token_symbol:
            price_usd = self.get_token_price_usd(token_symbol, chain, timestamp)

        # Calculate volume USD
        amount_in = parsed.get('amount_in', 0)
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
                volume_data_source = 'block_explorer',
                updated_at = NOW()
            WHERE tx_hash = %s
        """, (
            swap_volume_usd,
            token_symbol,
            token_address,
            amount_in,
            tx_hash
        ))

        logger.info(f"✓ {tx_hash}: {amount_in:.6f} {token_symbol} = ${swap_volume_usd:.2f if swap_volume_usd else 0} (method: {parsed.get('parsing_method')})")
        return True

    def enrich_missing_volumes(self, limit: int = None, test_mode: bool = False):
        """Enrich all Arkham records missing volume data."""
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        # Get records for chains we have API keys for
        available_chains = [chain for chain, cfg in EXPLORER_CONFIG.items() if cfg['api_key']]

        if not available_chains:
            logger.error("No API keys available!")
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

        logger.info(f"Found {len(records)} records to enrich")

        enriched_count = 0
        failed_count = 0

        for i, record in enumerate(records, 1):
            logger.info(f"\n[{i}/{len(records)}] {record['chain']} - {record['tx_hash'][:16]}...")

            try:
                success = self.enrich_record(record)

                if success:
                    enriched_count += 1
                else:
                    failed_count += 1

                # Commit every 10 records
                if i % 10 == 0:
                    self.db.commit()
                    logger.info(f"Progress: {enriched_count} enriched, {failed_count} failed")

            except Exception as e:
                logger.error(f"Error enriching {record['tx_hash']}: {e}")
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

    parser = argparse.ArgumentParser(description='Enrich Arkham records using block explorer APIs')
    parser.add_argument('--check-keys', action='store_true', help='Check API key status and exit')
    parser.add_argument('--test', action='store_true', help='Test mode: only 5 records')
    parser.add_argument('--limit', type=int, help='Limit number of records')

    args = parser.parse_args()

    try:
        enricher = ExplorerEnricher()

        # Always show API key status first
        if not enricher.check_api_keys():
            if args.check_keys:
                sys.exit(0)
            else:
                logger.error("\nCannot proceed without API keys!")
                sys.exit(1)

        if args.check_keys:
            sys.exit(0)

        # Proceed with enrichment
        if args.test:
            logger.info("\nTEST MODE: Enriching 5 records")
            enricher.enrich_missing_volumes(limit=5, test_mode=True)
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
