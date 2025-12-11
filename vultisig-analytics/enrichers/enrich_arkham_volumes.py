#!/usr/bin/env python3
"""
Enrich Arkham DEX aggregator records with swap volume data.
Uses volume_extractor.js to parse blockchain transactions and extract swap volumes.
"""

import os
import sys
import json
import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

class VolumeEnricher:
    def __init__(self):
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")

        self.db = psycopg2.connect(DATABASE_URL)
        self.extractor_path = os.path.join(
            os.path.dirname(__file__),
            'ingestors',
            'volume_extractor.js'
        )

        if not os.path.exists(self.extractor_path):
            raise FileNotFoundError(f"volume_extractor.js not found at {self.extractor_path}")

    def call_volume_extractor(self, tx_hash: str, chain: str) -> dict:
        """
        Call volume_extractor.js to extract volume from a transaction.

        Returns:
            {
                'amount': '500000000000000000',  # Raw amount
                'token': '0x...',  # Token address or 'NATIVE'
                'tokenSymbol': 'ETH',  # Token symbol
                'tokenOut': '0x...',  # Destination token address (for swaps)
                'tokenOutSymbol': 'USDC',  # Destination token symbol
                'type': '1inch_swap',  # Transaction type
                'decimals': 18  # Token decimals
            }
        """
        try:
            # Create a simple Node.js wrapper to call the extractor
            wrapper_code = f"""
            const VolumeExtractor = require('{self.extractor_path}');
            const extractor = new VolumeExtractor();

            (async () => {{
                try {{
                    const result = await extractor.getVolume('{tx_hash}', '{chain}');
                    console.log(JSON.stringify(result));
                }} catch (err) {{
                    console.error(JSON.stringify({{ error: err.message }}));
                    process.exit(1);
                }}
            }})();
            """

            # Execute Node.js code - pass current environment variables (including loaded .env)
            result = subprocess.run(
                ['node', '-e', wrapper_code],
                capture_output=True,
                text=True,
                timeout=30,
                env=os.environ.copy()  # Pass env vars to Node.js subprocess
            )

            if result.returncode != 0:
                logger.error(f"Volume extractor failed for {tx_hash}: {result.stderr}")
                return None

            # Parse JSON output
            output = result.stdout.strip()
            if not output or output == 'null':
                return None

            return json.loads(output)

        except subprocess.TimeoutExpired:
            logger.error(f"Volume extraction timeout for {tx_hash}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse volume extractor output for {tx_hash}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error calling volume extractor for {tx_hash}: {e}")
            return None

    def convert_to_human_readable(self, raw_amount: str, decimals: int) -> float:
        """Convert raw token amount to human-readable value."""
        try:
            return int(raw_amount) / (10 ** decimals)
        except:
            return 0.0

    def get_token_symbol_from_address(self, token_address: str, chain: str) -> str:
        """
        Get token symbol from address using database lookup.

        Args:
            token_address: Token contract address
            chain: Blockchain name (e.g., 'Ethereum', 'BSC')

        Returns:
            Token symbol or None if not found
        """
        if token_address == 'NATIVE' or not token_address:
            chain_native_tokens = {
                'Ethereum': 'ETH',
                'BSC': 'BNB',
                'Polygon': 'MATIC',
                'Arbitrum': 'ETH',
                'Optimism': 'ETH',
                'Base': 'ETH',
                'Avalanche': 'AVAX',
                'Blast': 'ETH'
            }
            return chain_native_tokens.get(chain)

        # Try to get token symbol from dex_aggregator_revenue table
        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT DISTINCT token_in_symbol
            FROM dex_aggregator_revenue
            WHERE token_in_address = %s AND token_in_symbol IS NOT NULL
            LIMIT 1
        """, (token_address,))
        result = cursor.fetchone()
        if result:
            return result['token_in_symbol']

        # Try token_out_address as fallback
        cursor.execute("""
            SELECT DISTINCT token_out_symbol
            FROM dex_aggregator_revenue
            WHERE token_out_address = %s AND token_out_symbol IS NOT NULL
            LIMIT 1
        """, (token_address,))
        result = cursor.fetchone()
        if result:
            return result['token_out_symbol']

        return None

    def get_token_price_usd(self, token_symbol: str, chain: str, timestamp: datetime) -> float:
        """
        Get token price in USD from historical_prices table.
        Falls back to current price if historical not available.
        """
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        # Try to find price around the transaction timestamp
        cursor.execute("""
            SELECT price_usd
            FROM historical_prices
            WHERE token_symbol = %s
              AND chain = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (token_symbol, chain, timestamp))

        result = cursor.fetchone()
        if result:
            return float(result['price_usd'])

        # Fallback: Try without chain restriction
        cursor.execute("""
            SELECT price_usd
            FROM historical_prices
            WHERE token_symbol = %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (token_symbol, timestamp))

        result = cursor.fetchone()
        if result:
            return float(result['price_usd'])

        # No price found
        logger.warning(f"No price found for {token_symbol} on {chain} at {timestamp}")
        return 0.0

    def enrich_record(self, record: dict) -> bool:
        """
        Enrich a single Arkham record with volume data from blockchain.

        Returns:
            True if successfully enriched, False otherwise
        """
        tx_hash = record['tx_hash']
        chain = record['chain']
        timestamp = record['timestamp']

        logger.info(f"Enriching {tx_hash} on {chain}")

        # Call volume extractor
        volume_data = self.call_volume_extractor(tx_hash, chain)

        if not volume_data:
            logger.warning(f"Could not extract volume for {tx_hash}")
            return False

        # Extract data
        raw_amount = volume_data.get('amount')
        token_address = volume_data.get('token')
        token_out_address = volume_data.get('tokenOut')  # Extract destination token
        decimals = volume_data.get('decimals', 18)
        tx_type = volume_data.get('type')

        if not raw_amount:
            logger.warning(f"No amount found in extracted data for {tx_hash}")
            return False

        # Convert to human-readable
        amount_in = self.convert_to_human_readable(raw_amount, decimals)

        # Get token symbols from extractor (more reliable than database lookup)
        token_in_symbol = volume_data.get('tokenSymbol')
        token_out_symbol = volume_data.get('tokenOutSymbol')

        # Fallback: try database lookup if extractor didn't provide symbols
        if not token_in_symbol and token_address:
            token_in_symbol = self.get_token_symbol_from_address(token_address, chain)

        if not token_out_symbol and token_out_address:
            token_out_symbol = self.get_token_symbol_from_address(token_out_address, chain)

        # Handle NATIVE token symbol conversion
        if token_in_symbol == 'NATIVE':
            chain_native_tokens = {
                'Ethereum': 'ETH',
                'BSC': 'BNB',
                'Polygon': 'MATIC',
                'Arbitrum': 'ETH',
                'Optimism': 'ETH',
                'Base': 'ETH',
                'Avalanche': 'AVAX',
                'Blast': 'ETH'
            }
            token_in_symbol = chain_native_tokens.get(chain, token_in_symbol)

        # Calculate volume in USD using the same logic as existing records
        # Vultisig takes 0.5% fee, so: volume = fee / 0.005 = fee * 200
        # This matches the existing data pattern (volume_to_fee_ratio = 200x)
        swap_volume_usd = record['actual_fee_usd'] * 200 if record.get('actual_fee_usd') else None

        # Update database with both token_in and token_out
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE dex_aggregator_revenue
            SET
                swap_volume_usd = %s,
                token_in_symbol = %s,
                token_in_address = %s,
                token_out_symbol = %s,
                token_out_address = %s,
                amount_in = %s,
                volume_data_source = 'blockchain_rpc',
                updated_at = NOW()
            WHERE tx_hash = %s
        """, (
            swap_volume_usd,
            token_in_symbol,
            token_address,
            token_out_symbol,
            token_out_address,
            amount_in,
            tx_hash
        ))

        # Log with swap path if available
        volume_str = f"${swap_volume_usd:.2f}" if swap_volume_usd else "N/A"
        if token_out_symbol:
            logger.info(f"✓ Enriched {tx_hash}: {token_in_symbol} → {token_out_symbol} ({amount_in:.4f} {token_in_symbol}, volume: {volume_str})")
        else:
            logger.info(f"✓ Enriched {tx_hash}: {amount_in:.4f} {token_in_symbol} (volume: {volume_str})")
        return True

    def enrich_all_missing_volumes(self, limit: int = None):
        """
        Enrich all Arkham records that are missing volume data.

        Args:
            limit: Maximum number of records to enrich (None = all)
        """
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        # Get records without token symbol data (what the dashboard needs for filtering)
        # Records may have swap_volume_usd from Arkham but still need token symbols
        query = """
            SELECT tx_hash, chain, timestamp, actual_fee_usd, swap_volume_usd
            FROM dex_aggregator_revenue
            WHERE (token_in_symbol IS NULL OR token_out_symbol IS NULL)
              AND protocol = '1inch'
            ORDER BY swap_volume_usd DESC NULLS LAST
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        records = cursor.fetchall()

        if not records:
            logger.info("No records to enrich!")
            return

        logger.info(f"Found {len(records)} records to enrich")

        enriched_count = 0
        failed_count = 0

        for i, record in enumerate(records, 1):
            logger.info(f"\n[{i}/{len(records)}] Processing {record['tx_hash']}")

            try:
                success = self.enrich_record(record)

                if success:
                    enriched_count += 1
                else:
                    failed_count += 1

                # Commit every 10 records
                if i % 10 == 0:
                    self.db.commit()
                    logger.info(f"Committed progress: {enriched_count} enriched, {failed_count} failed")

            except Exception as e:
                logger.error(f"Error enriching {record['tx_hash']}: {e}")
                failed_count += 1
                self.db.rollback()

        # Final commit
        self.db.commit()

        logger.info(f"\n{'='*80}")
        logger.info("ENRICHMENT COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Total records processed: {len(records)}")
        logger.info(f"Successfully enriched: {enriched_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Success rate: {enriched_count/len(records)*100:.1f}%")

    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Enrich Arkham records with swap volume data')
    parser.add_argument('--limit', type=int, help='Limit number of records to enrich')
    parser.add_argument('--test', action='store_true', help='Test mode: only enrich 5 records')

    args = parser.parse_args()

    try:
        enricher = VolumeEnricher()

        if args.test:
            logger.info("TEST MODE: Enriching 5 records only")
            enricher.enrich_all_missing_volumes(limit=5)
        else:
            enricher.enrich_all_missing_volumes(limit=args.limit)

        enricher.close()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
