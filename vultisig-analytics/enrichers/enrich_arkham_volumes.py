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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config  # noqa: E402
from ingestors.swapkit_senders import (  # noqa: E402
    is_swapkit_payout_sender,
    plan_swapkit_enrichment,
)
from utils.price_fetcher import PriceFetcher  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

# Vultisig charges 50 bps on aggregator swaps, so volume = fee / 0.005.
# Matches the volume_to_fee_ratio of historical Arkham-sourced rows.
VOLUME_TO_FEE_MULTIPLIER = config.AFFILIATE_VOLUME_TO_FEE_MULTIPLIER

# CoinGecko ids for fee tokens we can price. Fees in unmapped (long-tail)
# tokens stay at 0 — the row is still marked processed so it never loops.
COINGECKO_IDS = {
    'ETH': 'ethereum', 'WETH': 'ethereum',
    'BNB': 'binancecoin', 'WBNB': 'binancecoin',
    'MATIC': 'matic-network', 'WMATIC': 'matic-network',
    'POL': 'polygon-ecosystem-token',
    'AVAX': 'avalanche-2', 'WAVAX': 'avalanche-2',
    'USDC': 'usd-coin', 'USDC.E': 'usd-coin',
    'USDT': 'tether',
    'DAI': 'dai',
    'WBTC': 'wrapped-bitcoin',
    'VULT': 'vultisig',
    'INJ': 'injective-protocol',
}

NATIVE_TOKENS = {
    'Ethereum': 'ETH',
    'BSC': 'BNB',
    'Polygon': 'MATIC',
    'Arbitrum': 'ETH',
    'Optimism': 'ETH',
    'Base': 'ETH',
    'Avalanche': 'AVAX',
    'Blast': 'ETH',
}

class VolumeEnricher:
    def __init__(self):
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")

        self.db = psycopg2.connect(DATABASE_URL)
        self.price_fetcher = PriceFetcher(DATABASE_URL)
        self.extractor_path = os.path.join(
            os.path.dirname(__file__),
            '..',
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
            return NATIVE_TOKENS.get(chain)

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

    def resolve_fee_usd(self, record: dict) -> float:
        """USD value of the fee transfer at tx time.

        Etherscan-ingested rows land with actual_fee_usd = 0 (the ingestor
        can't price); the fee IS the transfer itself, so price its token via
        CoinGecko. Unmapped tokens return 0 — fee unknown.
        """
        existing = record.get('actual_fee_usd')
        if existing:
            return float(existing)

        token_id = COINGECKO_IDS.get((record.get('fee_token_symbol') or '').upper())
        if not token_id or not record.get('fee_amount_raw'):
            return 0.0

        price = self.price_fetcher.get_historical_price(token_id, record['timestamp'].date())
        if not price:
            return 0.0
        return float(record['fee_amount_raw']) * price

    def extract_swap_path(self, record: dict) -> dict:
        """Token in/out symbols + amounts for the swap, via volume_extractor.js.
        Returns {} when the tx can't be parsed — path display stays empty."""
        volume_data = self.call_volume_extractor(record['tx_hash'], record['chain'])
        if not volume_data or not volume_data.get('amount'):
            return {}

        chain = record['chain']
        token_address = volume_data.get('token')
        token_out_address = volume_data.get('tokenOut')
        token_in_symbol = volume_data.get('tokenSymbol')
        token_out_symbol = volume_data.get('tokenOutSymbol')

        if not token_in_symbol and token_address:
            token_in_symbol = self.get_token_symbol_from_address(token_address, chain)
        if not token_out_symbol and token_out_address:
            token_out_symbol = self.get_token_symbol_from_address(token_out_address, chain)
        if token_in_symbol == 'NATIVE':
            token_in_symbol = NATIVE_TOKENS.get(chain, token_in_symbol)

        return {
            'token_in_symbol': token_in_symbol,
            'token_in_address': token_address,
            'token_out_symbol': token_out_symbol,
            'token_out_address': token_out_address,
            'amount_in': self.convert_to_human_readable(
                volume_data['amount'], volume_data.get('decimals', 18)
            ),
        }

    def apply_updates(self, tx_hash: str, updates: dict, stamp: str = 'estimated') -> None:
        """Persist enrichment. Stamp is the loop-breaker for unpriceable/payout rows."""
        updates = {**updates, 'volume_data_source': stamp}
        set_clause = ', '.join(f"{col} = %s" for col in updates)
        cursor = self.db.cursor()
        cursor.execute(
            f"UPDATE dex_aggregator_revenue SET {set_clause}, updated_at = NOW() WHERE tx_hash = %s",
            (*updates.values(), tx_hash),
        )

    def enrich_record(self, record: dict) -> bool:
        """Price the fee, estimate volume, and fill the swap path for one row.

        Returns True if anything beyond the processed-stamp was written.
        """
        tx_hash = record['tx_hash']
        logger.info(f"Enriching {tx_hash} on {record['chain']}")

        updates = {}
        stamp = 'estimated'
        is_payout = is_swapkit_payout_sender(record.get('from_address'))
        if record.get('protocol') == config.SWAPKIT_PROTOCOL:
            fee_usd = self.resolve_fee_usd(record)
            planned, stamp = plan_swapkit_enrichment(
                record.get('from_address'),
                fee_usd,
                float(record.get('actual_fee_usd') or 0),
            )
            updates.update(planned)
        elif record.get('swap_volume_usd') is None:
            fee_usd = self.resolve_fee_usd(record)
            if fee_usd:
                updates['swap_volume_usd'] = fee_usd * VOLUME_TO_FEE_MULTIPLIER
                if not record.get('actual_fee_usd'):
                    updates['actual_fee_usd'] = fee_usd

        if not is_payout and (
            not record.get('token_in_symbol') or not record.get('token_out_symbol')
        ):
            updates.update(self.extract_swap_path(record))

        self.apply_updates(tx_hash, updates, stamp=stamp)

        path = f"{updates.get('token_in_symbol', '?')} → {updates.get('token_out_symbol', '?')}"
        volume = updates.get('swap_volume_usd')
        volume_str = f"${volume:.2f}" if volume else "N/A"
        logger.info(f"✓ Enriched {tx_hash}: {path} (volume: {volume_str})")
        return bool(updates)

    def enrich_all_missing_volumes(self, limit: int = None):
        """
        Enrich all Arkham records that are missing volume data.

        Args:
            limit: Maximum number of records to enrich (None = all)
        """
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        # Rows that still need enrichment AND haven't been processed before.
        # Etherscan rows wait for the router classifier ('router_check') so we
        # never enrich a row that later gets demoted; processing stamps
        # 'estimated', which removes the row from this scope permanently —
        # rows with unpriceable fee tokens must not be retried every cycle.
        query = """
            SELECT tx_hash, chain, timestamp, actual_fee_usd, swap_volume_usd,
                   token_in_symbol, token_out_symbol,
                   fee_token_symbol, fee_amount_raw, fee_data_source,
                   protocol, from_address
            FROM dex_aggregator_revenue
            WHERE (swap_volume_usd IS NULL OR token_in_symbol IS NULL OR token_out_symbol IS NULL)
              AND protocol IN %s
              AND volume_data_source IS DISTINCT FROM 'estimated'
              AND volume_data_source IS DISTINCT FROM 'payout'
              AND volume_data_source IS DISTINCT FROM 'unpriced'
              AND (
                    (fee_data_source = 'etherscan' AND volume_data_source = 'router_check')
                 OR (fee_data_source = 'etherscan' AND protocol = %s)
                 OR (fee_data_source IS DISTINCT FROM 'etherscan'
                     AND (token_in_symbol IS NULL OR token_out_symbol IS NULL))
              )
            ORDER BY timestamp DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query, (config.DEX_REVENUE_PROVIDERS, config.SWAPKIT_PROTOCOL))
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
